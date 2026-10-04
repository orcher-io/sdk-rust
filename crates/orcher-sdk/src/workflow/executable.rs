//! How workflow steps run: registered tasks and inline closures.
//!
//! Both kinds of step are journaled. On replay, a step whose result is already
//! in the journal returns that result instead of running again, which keeps
//! workflow code deterministic across runs.
//!
//! - Registered tasks (`execute_task`) are declared with `#[task]`, have typed
//!   inputs and outputs, and are dispatched by the engine to a worker.
//! - Inline closures (`execute`) are defined in the workflow body and run in the
//!   workflow process. Use them for one-off calls that do not merit a task.
//!
//! ## Usage
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[task(retry = 3)]
//! async fn send_email(_ctx: TaskContext, to: String) -> Result<String> {
//!     Ok(format!("sent to {to}"))
//! }
//!
//! #[workflow]
//! async fn notify(ctx: WorkflowContext, user_id: String) -> Result<String> {
//!     // An inline closure
//!     let email: String = ctx
//!         .execute("look_up_email", move || async move { Ok(format!("{user_id}@example.com")) })
//!         .await?;
//!
//!     // A registered task
//!     ctx.execute_task(send_email, email).await
//! }
//! ```

use crate::error::{Error, Result};
use crate::workflow::WorkflowContext;
use metrics::{counter, histogram};
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::time::Instant;

/// A step that can run inside a workflow and be replayed from the journal.
///
/// [`TaskExecution`] runs a registered `#[task]`; [`ClosureExecution`] runs an
/// inline closure. Both journal their result, and on replay both return the
/// journaled result instead of running again.
///
/// ## Example
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// # #[task]
/// # async fn my_task(_ctx: TaskContext, input: String) -> Result<String> { Ok(input) }
/// # async fn example(ctx: &WorkflowContext) -> Result<()> {
/// // A registered task
/// let result: String = ctx.execute_task(my_task, "input".to_string()).await?;
///
/// // An inline closure
/// let result: i32 = ctx.execute("fetch_data", || async { Ok(42) }).await?;
/// # Ok(())
/// # }
/// ```
pub trait Executable<O> {
    /// Runs the step, or returns its journaled result when replaying.
    ///
    /// `step_name` names the step; combined with the context's sequence
    /// number it forms the journal key.
    ///
    /// # Errors
    ///
    /// Returns an error if the step fails or its input or result cannot be
    /// serialized or deserialized. A task that has not completed yet returns a
    /// suspension error so the runtime can schedule it.
    fn execute_in_context(
        self,
        ctx: &WorkflowContext,
        step_name: &str,
    ) -> impl Future<Output = Result<O>> + Send;
}

// ============================================================================
// Implementation: Task Execution (from registry)
// ============================================================================

/// A step that runs a registered task with the given input.
///
/// The SDK does not run the task itself. It asks the engine to schedule the
/// task on a worker, using the retry policy and limits declared on `#[task]`,
/// and returns the result once the engine journals it.
pub struct TaskExecution<I, O> {
    input: I,
    _output: PhantomData<O>,
}

impl<I, O> TaskExecution<I, O>
where
    I: Serialize + Send + Sync + 'static,
    O: DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a task step with the given input.
    pub fn new(input: I) -> Self {
        Self {
            input,
            _output: PhantomData,
        }
    }
}

// ============================================================================
// Implementation: Task Execution (from registry)
// ============================================================================

impl<I, O> Executable<O> for TaskExecution<I, O>
where
    I: Serialize + Send + Sync + 'static,
    O: DeserializeOwned + Send + Sync + 'static,
{
    async fn execute_in_context(self, ctx: &WorkflowContext, step_name: &str) -> Result<O> {
        use crate::workflow::command::{ScheduleTaskCommand, TaskRetryPolicy};
        use std::time::Duration;

        let start = Instant::now();

        counter!(
            "orcher.workflow.execute.task.calls",
            "step_name" => step_name.to_string()
        )
        .increment(1);

        // One number from the step counter names the step and is its command's sequence.
        // It is taken once, on every path, so the id is identical on every replay as
        // long as the workflow issues its steps in the same order.
        let sequence = ctx.next_sequence();
        let task_id = format!("{}_{}", step_name, sequence);
        ctx.reach_step(&task_id);

        // When replaying, the engine has sent the journaled results of completed
        // tasks; look this one up.
        let state = ctx.state.lock().unwrap();
        let replaying = state.replaying;
        let cached_result = state.pending_task_results.get(&task_id).cloned();
        if replaying && cached_result.is_some() {
            // Received: the workflow's clock moves to when it was journaled.
            state.observe(&task_id);
        }

        tracing::warn!(
            task_id = %task_id,
            step_name = %step_name,
            replaying = replaying,
            has_cached = cached_result.is_some(),
            cached_len = cached_result.as_ref().map(|v| v.len()).unwrap_or(0),
            total_pending = state.pending_task_results.len(),
            "🔍 TASK_EXEC: Checking for cached result"
        );

        drop(state); // Do not hold the state lock across an await.

        if replaying {
            if let Some(result_bytes) = cached_result {
                // A journaled result is never empty; an empty one means the result
                // was lost when it was injected into the context.
                if result_bytes.is_empty() {
                    tracing::error!(
                        task_id = %task_id,
                        step_name = %step_name,
                        "🔴 TASK_EXEC: Found EMPTY cached result during replay - this should never happen!"
                    );
                    return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                        format!(
                            "Invalid replay state: Task '{}' has empty result in cache. \
                             This indicates a bug in result injection or journal conversion.",
                            step_name
                        ),
                    )));
                }

                // The engine journals a task that failed for good as a JSON sentinel
                // result (see TASK_FAILED_SENTINEL_KEY). Raise it as a catchable
                // TaskFailed error rather than deserializing it as a success value, so
                // workflow code and saga compensation can observe the failure.
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&result_bytes) {
                    if v.get(crate::error::TASK_FAILED_SENTINEL_KEY)
                        .and_then(|b| b.as_bool())
                        .unwrap_or(false)
                    {
                        let reason = v
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("task failed")
                            .to_string();
                        tracing::info!(
                            task_id = %task_id,
                            step_name = %step_name,
                            reason = %reason,
                            "Replaying terminal task failure from journal"
                        );
                        return Err(Error::Workflow(crate::error::WorkflowError::TaskFailed {
                            task_type: step_name.to_string(),
                            attempts: v.get("attempts").and_then(|a| a.as_u64()).unwrap_or(1)
                                as u32,
                            reason,
                        }));
                    }
                }

                counter!(
                    "orcher.workflow.execute.cache_hits",
                    "step_name" => step_name.to_string(),
                    "step_type" => "task"
                )
                .increment(1);

                tracing::debug!(
                    task_id = %task_id,
                    step_name = %step_name,
                    result_len = result_bytes.len(),
                    "Replaying task execution from journal"
                );

                let output: O = serde_json::from_slice(&result_bytes).map_err(|e| {
                    tracing::error!(
                        task_id = %task_id,
                        step_name = %step_name,
                        result_len = result_bytes.len(),
                        result_preview = ?&result_bytes[..result_bytes.len().min(100)],
                        error = %e,
                        "🔴 TASK_EXEC: Failed to deserialize cached result"
                    );
                    Error::Serialization(format!(
                        "Failed to deserialize task result for '{}': {} (result length: {})",
                        step_name,
                        e,
                        result_bytes.len()
                    ))
                })?;

                histogram!(
                    "orcher.workflow.step.duration_ms",
                    "step_type" => "task",
                    "step_name" => step_name.to_string(),
                    "cached" => "true"
                )
                .record(start.elapsed().as_millis() as f64);

                return Ok(output);
            }
            // No journaled result: replay has caught up with the journal and this is
            // the next step to run. Fall through to schedule it.
            tracing::info!(
                task_id = %task_id,
                step_name = %step_name,
                "Task result not in cache during replay - this is a new step, will execute fresh"
            );
        }

        counter!(
            "orcher.workflow.execute.cache_misses",
            "step_name" => step_name.to_string(),
            "step_type" => "task"
        )
        .increment(1);

        let input_bytes = serde_json::to_vec(&self.input).map_err(|e| {
            Error::Serialization(format!(
                "Failed to serialize task input for '{}': {}",
                step_name, e
            ))
        })?;

        tracing::debug!(
            task_id = %task_id,
            step_name = %step_name,
            "Scheduling task execution (will be sent to server)"
        );

        // The task is not run here. A ScheduleTask command goes to the engine,
        // which journals the schedule, dispatches the task to a worker on the
        // queue, and journals the result when the task completes.
        // A queue override set for this step applies to it only.
        let task_queue = {
            let mut state = ctx.state.lock().unwrap();
            state
                .task_queue_override
                .take()
                .unwrap_or_else(|| ctx.execution().task_queue.clone())
        };
        // Use the retry policy declared on the task (#[task(retry = N)] or
        // retry_policy(...)); tasks without one get the default below.
        let retry_policy = crate::worker::registration::task_retry_policy(step_name)
            .map(|p| TaskRetryPolicy {
                max_attempts: p.max_attempts,
                initial_interval: p.initial_interval,
                max_interval: p.max_interval,
                backoff_coefficient: p.backoff_coefficient,
                non_retryable_errors: p.non_retryable_errors,
            })
            .unwrap_or(TaskRetryPolicy {
                max_attempts: 3,
                initial_interval: Duration::from_secs(1),
                max_interval: Duration::from_secs(60),
                backoff_coefficient: 2.0,
                non_retryable_errors: vec![],
            });
        let declared = crate::worker::registration::task_limits(step_name);
        let command = ScheduleTaskCommand {
            sequence,
            task_id: task_id.clone(),
            task_type: step_name.to_string(),
            task_queue,
            input: input_bytes,
            // Limits declared on `#[task(timeout = .., heartbeat_timeout = ..)]`.
            // Without a declared timeout the task gets 300 seconds.
            timeout: declared
                .and_then(|d| d.timeout_secs)
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(300)),
            heartbeat_timeout: declared
                .and_then(|d| d.heartbeat_timeout_secs)
                .map(Duration::from_secs),
            queue_timeout: None,
            retry_policy: Some(retry_policy),
            headers: vec![],
        };

        // Mark the step as a task so its result is not recorded as a closure.
        {
            let mut state = ctx.state.lock().unwrap();
            state.step_type_map.insert(task_id.clone(), 0); // 0 = Task
        }

        ctx.add_command(crate::workflow::command::WorkflowCommand::ScheduleTask(
            command,
        ));

        histogram!(
            "orcher.workflow.step.duration_ms",
            "step_type" => "task",
            "step_name" => step_name.to_string(),
            "cached" => "false"
        )
        .record(start.elapsed().as_millis() as f64);

        // Suspend the workflow. The executor catches this error, sends the queued
        // commands to the engine, and replays the workflow once the task completes;
        // the result is then found in the journal above.
        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Task '{}' scheduled for execution", step_name),
            pending_operations: vec![task_id],
        }))
    }
}

// ============================================================================
// Implementation: Closure Execution (inline operations)
// ============================================================================

/// A step that runs an inline closure in the workflow process.
///
/// Use it for one-off calls such as an HTTP request or a database query. The
/// closure runs locally, not on a worker. Its result is journaled when the
/// workflow run completes; after that, replay returns the journaled result and
/// the closure does not run again. A closure that has run but whose result has
/// not been journaled yet may run again, so it should be safe to repeat.
///
/// ## Example
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// #[derive(Serialize, Deserialize)]
/// struct User {
///     name: String,
/// }
///
/// async fn fetch_user(id: u64) -> Result<User> {
///     // An HTTP call in a real program.
///     Ok(User { name: format!("user-{id}") })
/// }
///
/// # async fn example(ctx: &WorkflowContext) -> Result<User> {
/// let user: User = ctx.execute("fetch_user_api", || fetch_user(123)).await?;
/// # Ok(user)
/// # }
/// ```
pub struct ClosureExecution<F, Fut, O> {
    closure: F,
    _marker: PhantomData<(Fut, O)>,
}

impl<F, Fut, O> ClosureExecution<F, Fut, O>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<O>> + Send,
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a closure step. `ctx.execute()` calls this for you.
    pub fn new(closure: F) -> Self {
        Self {
            closure,
            _marker: PhantomData,
        }
    }
}

impl<F, Fut, O> Executable<O> for ClosureExecution<F, Fut, O>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<O>> + Send,
    O: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn execute_in_context(self, ctx: &WorkflowContext, step_name: &str) -> Result<O> {
        let start = Instant::now();

        counter!(
            "orcher.workflow.execute.closure.calls",
            "step_name" => step_name.to_string()
        )
        .increment(1);

        // Sequence-based, so the id is the same on every replay.
        let step_id = format!("{}_{}", step_name, ctx.next_sequence());

        let (replaying, cached_result) = {
            let state = ctx.state.lock().unwrap();
            let replaying = state.replaying;
            let cached_result = state.pending_task_results.get(&step_id).cloned();
            (replaying, cached_result)
        }; // Released before the closure is awaited.

        if replaying {
            if let Some(result_bytes) = cached_result {
                counter!(
                    "orcher.workflow.execute.cache_hits",
                    "step_name" => step_name.to_string(),
                    "step_type" => "closure"
                )
                .increment(1);

                tracing::debug!(
                    step_id = %step_id,
                    step_name = %step_name,
                    "Replaying closure execution from journal"
                );

                let output: O = serde_json::from_slice(&result_bytes).map_err(|e| {
                    Error::Serialization(format!(
                        "Failed to deserialize closure result for '{}': {}",
                        step_name, e
                    ))
                })?;

                histogram!(
                    "orcher.workflow.step.duration_ms",
                    "step_type" => "closure",
                    "step_name" => step_name.to_string(),
                    "cached" => "true"
                )
                .record(start.elapsed().as_millis() as f64);

                return Ok(output);
            }

            // No journaled result: replay has caught up and this is the next step.
            tracing::info!(
                step_id = %step_id,
                step_name = %step_name,
                "Closure result not in cache during replay - this is a new step, will execute fresh"
            );
        }

        counter!(
            "orcher.workflow.execute.cache_misses",
            "step_name" => step_name.to_string(),
            "step_type" => "closure"
        )
        .increment(1);

        tracing::debug!(
            step_id = %step_id,
            step_name = %step_name,
            "Executing closure (fresh execution)"
        );

        let future = (self.closure)();
        let exec_result = future.await;

        // Failed attempts count too.
        {
            let mut state = ctx.state.lock().unwrap();
            let count = state
                .step_attempt_counts
                .entry(step_id.clone())
                .or_insert(0);
            *count += 1;
        }

        let result = match exec_result {
            Ok(r) => r,
            Err(e) => {
                // Record the failure before propagating the error, so
                // extract_failed_closure_commands can report which step failed.
                let failure = orcher_proto::orcher::v1::Failure {
                    message: e.to_string(),
                    source: "ClosureExecution".to_string(),
                    failure_type: "ClosureError".to_string(),
                    ..Default::default()
                };
                {
                    let mut state = ctx.state.lock().unwrap();
                    state.pending_task_failures.insert(step_id.clone(), failure);
                    state.step_type_map.insert(step_id.clone(), 1);
                }
                return Err(e);
            }
        };

        let result_bytes = serde_json::to_vec(&result).map_err(|e| {
            Error::Serialization(format!(
                "Failed to serialize closure result for '{}': {}",
                step_name, e
            ))
        })?;

        // The result is sent to the engine for journaling when the run completes
        // (see extract_closure_commands).
        {
            let mut state = ctx.state.lock().unwrap();
            state
                .pending_task_results
                .insert(step_id.clone(), result_bytes);
            state.step_type_map.insert(step_id.clone(), 1); // 1 = Closure
        }

        tracing::info!(
            step_id = %step_id,
            step_name = %step_name,
            duration_ms = start.elapsed().as_millis(),
            "Closure executed successfully"
        );

        histogram!(
            "orcher.workflow.step.duration_ms",
            "step_type" => "closure",
            "step_name" => step_name.to_string(),
            "cached" => "false"
        )
        .record(start.elapsed().as_millis() as f64);

        Ok(result)
    }
}

// ============================================================================
// Helper Functions for Type Inference
// ============================================================================

/// Wraps a task input in a [`TaskExecution`]; used by `ctx.execute()`.
#[doc(hidden)]
pub fn task_execution<I, O>(input: I) -> TaskExecution<I, O>
where
    I: Serialize + Send + Sync + 'static,
    O: DeserializeOwned + Send + Sync + 'static,
{
    TaskExecution::new(input)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_execution_construction() {
        let _task_exec = TaskExecution::<String, String>::new("input".to_string());
    }
}
