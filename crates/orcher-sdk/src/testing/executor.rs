//! [`TestWorkflowExecutor`], which runs a workflow in memory and serves its tasks from
//! mocks, without an Orcher server.

use crate::error::{Error, Result};
use crate::testing::mocks::MockRegistry;
use crate::workflow::{WorkflowCommand, WorkflowContext, WorkflowExecution};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Runs a workflow in memory, serving its tasks and child workflows from mocks, and
/// skipping through its timers.
///
/// The executor keeps a journal of what the workflow has been given: task results,
/// child workflow outcomes, fired timers and delivered events. Each time the workflow
/// suspends, it resolves what the workflow scheduled and replays the workflow from the
/// start against the journal, the same way a worker replays history.
///
/// - **Tasks** are answered by the mocks passed to [`new`](Self::new). A mock that
///   fails the task reaches the workflow as `TaskFailed`.
/// - **Child workflows** are answered by the mocks passed to
///   [`with_child_mocks`](Self::with_child_mocks), keyed by child workflow type. A
///   mock that fails reaches the parent as `ChildWorkflowFailed`.
/// - **Events** are the ones given with [`with_event`](Self::with_event) and
///   [`with_event_after`](Self::with_event_after).
/// - **Time** is virtual and starts when the run starts. Tasks and child workflows
///   finish instantly. When the workflow can make no progress until something
///   happens, the executor jumps the clock to the earliest timer or event due and
///   delivers it, so a `sleep` of a day returns at once with the workflow's clock a
///   day later. [`elapsed`](Self::elapsed) reports how much time a run skipped.
///
/// A missing mock, a mock whose input or result does not serialize, or a workflow left
/// waiting with no timer or event to come stops the run with an error.
pub struct TestWorkflowExecutor {
    mocks: Arc<RwLock<MockRegistry>>,
    /// Mocks for child workflows, keyed by child workflow type.
    child_mocks: Arc<RwLock<MockRegistry>>,
    /// Events to deliver, with how long after the start each is sent.
    events: Vec<(Duration, String, Vec<u8>)>,
    /// Replay limit, so a workflow that keeps scheduling tasks cannot loop forever
    max_steps: usize,
    /// Log each step through `tracing`
    verbose: bool,
    /// Virtual time the last run skipped.
    elapsed: Duration,
}

/// One thing the journal records the workflow as having been given, with the
/// virtual time (ms since the epoch) it was recorded at.
enum Entry {
    Task {
        id: String,
        result: Vec<u8>,
        at: i64,
    },
    Step {
        name: String,
        result: Vec<u8>,
        at: i64,
    },
    Child {
        id: String,
        result: Vec<u8>,
        at: i64,
    },
    Timer {
        id: String,
        at: i64,
    },
    Event {
        name: String,
        payload: Vec<u8>,
        at: i64,
    },
}

impl TestWorkflowExecutor {
    /// Create an executor over `mocks`, with a limit of 1000 steps.
    pub fn new(mocks: Arc<RwLock<MockRegistry>>) -> Self {
        Self {
            mocks,
            child_mocks: Arc::new(RwLock::new(MockRegistry::new())),
            events: Vec::new(),
            max_steps: 1000,
            verbose: false,
            elapsed: Duration::ZERO,
        }
    }

    /// Set how many times the workflow may be run before the executor gives up.
    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set whether to log each step through `tracing`.
    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Answer the child workflows the workflow starts from `mocks`, keyed by child
    /// workflow type.
    ///
    /// Register them with [`MockTaskBuilder`](crate::testing::MockTaskBuilder), naming
    /// the child workflow type where a task type would go. The child's input is what
    /// the mock receives, and what it returns is the child's result.
    pub fn with_child_mocks(mut self, mocks: Arc<RwLock<MockRegistry>>) -> Self {
        self.child_mocks = mocks;
        self
    }

    /// Send the event `name` to the workflow when it starts, as if the client sent it
    /// before the first run. The workflow receives it at its first matching
    /// `wait_for_event`.
    ///
    /// # Panics
    ///
    /// Panics if `payload` does not serialize to JSON.
    pub fn with_event<T: Serialize>(self, name: impl Into<String>, payload: T) -> Self {
        self.with_event_after(Duration::ZERO, name, payload)
    }

    /// Send the event `name` to the workflow `delay` after it starts, in virtual time.
    ///
    /// The executor reaches that time once the workflow is waiting with nothing else to
    /// do sooner, so a `wait_for_event_with_timeout` shorter than `delay` times out and
    /// one longer receives the event.
    ///
    /// # Panics
    ///
    /// Panics if `payload` does not serialize to JSON.
    pub fn with_event_after<T: Serialize>(
        mut self,
        delay: Duration,
        name: impl Into<String>,
        payload: T,
    ) -> Self {
        let payload = serde_json::to_vec(&payload).expect("event payload serializes to JSON");
        self.events.push((delay, name.into(), payload));
        self
    }

    /// Virtual time the last run skipped through timers and events.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Run a context-aware workflow to completion, serving its tasks and child
    /// workflows from mocks and skipping through its timers.
    ///
    /// # Errors
    ///
    /// Returns the workflow's own error, or an error if a scheduled task or child
    /// workflow has no mock, a mock's input or result does not serialize, the workflow
    /// is left waiting with no timer or event to come, or it takes more than the step
    /// limit.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use orcher_sdk::testing::{MockRegistry, MockTaskBuilder, TestWorkflowExecutor};
    /// # use orcher_sdk::workflow::WorkflowExecution;
    /// # use std::sync::{Arc, RwLock};
    /// # use std::time::Duration;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<()> {
    /// let mocks = Arc::new(RwLock::new(MockRegistry::new()));
    /// MockTaskBuilder::new("send_reminder".to_string(), mocks.clone()).returns(true);
    ///
    /// // Waits a day for approval, and sends a reminder if none comes.
    /// let approve = |ctx: WorkflowContext, _: ()| async move {
    ///     let day = Duration::from_secs(24 * 3600);
    ///     match ctx.wait_for_event_with_timeout::<bool>("approved", day).await? {
    ///         Some(approved) => Ok(approved),
    ///         None => {
    ///             let _: bool = ctx.execute_task("send_reminder", ()).await?;
    ///             Ok(false)
    ///         }
    ///     }
    /// };
    ///
    /// let execution = WorkflowExecution::new(
    ///     "wf-1".to_string(),
    ///     "run-1".to_string(),
    ///     "approve".to_string(),
    ///     "default".to_string(),
    ///     "approvals".to_string(),
    /// );
    ///
    /// // Approval comes after two hours: no reminder.
    /// let mut executor = TestWorkflowExecutor::new(mocks.clone())
    ///     .with_event_after(Duration::from_secs(2 * 3600), "approved", true);
    /// assert!(executor.execute_workflow(approve, execution.clone(), ()).await?);
    /// assert_eq!(executor.elapsed(), Duration::from_secs(2 * 3600));
    ///
    /// // No approval: the day passes, in no real time, and the reminder is sent.
    /// let mut executor = TestWorkflowExecutor::new(mocks);
    /// assert!(!executor.execute_workflow(approve, execution, ()).await?);
    /// assert_eq!(executor.elapsed(), Duration::from_secs(24 * 3600));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_workflow<F, Fut, I, O>(
        &mut self,
        workflow_fn: F,
        execution: WorkflowExecution,
        input: I,
    ) -> Result<O>
    where
        F: Fn(WorkflowContext, I) -> Fut,
        Fut: std::future::Future<Output = Result<O>>,
        I: Clone + Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    {
        let start_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut now_ms = start_ms;
        self.elapsed = Duration::ZERO;

        let mut journal: Vec<Entry> = Vec::new();
        // What has been resolved, so a command replayed on a later run is not
        // resolved twice.
        let mut resolved: HashSet<String> = HashSet::new();
        // When each timer the workflow has started is due, from the run that first
        // started it.
        let mut timer_due: BTreeMap<String, i64> = BTreeMap::new();
        // Events still to send, in the order they are due.
        let mut events: Vec<(i64, String, Vec<u8>)> = self
            .events
            .iter()
            .map(|(delay, name, payload)| {
                (
                    start_ms + delay.as_millis() as i64,
                    name.clone(),
                    payload.clone(),
                )
            })
            .collect();
        events.sort_by_key(|(at, _, _)| *at);
        let mut events = std::collections::VecDeque::from(events);
        // Events due at the start were sent before the first run.
        while events.front().is_some_and(|(at, _, _)| *at <= start_ms) {
            let (at, name, payload) = events.pop_front().unwrap();
            journal.push(Entry::Event { name, payload, at });
        }

        if self.verbose {
            tracing::info!(
                workflow_id = %execution.workflow_id,
                workflow_type = %execution.workflow_type,
                "Starting workflow execution in test mode"
            );
        }

        for step in 1..=self.max_steps {
            let context = Self::context_from_journal(&execution, start_ms, &journal);
            // Clones share state, so the commands the workflow schedules are readable
            // from this clone after the original has been moved into the workflow.
            let commands_from = context.clone();

            if self.verbose {
                tracing::debug!(
                    workflow_id = %execution.workflow_id,
                    step,
                    journal_entries = journal.len(),
                    "Executing workflow step"
                );
            }

            let error = match workflow_fn(context, input.clone()).await {
                Ok(output) => {
                    self.elapsed = Duration::from_millis((now_ms - start_ms).max(0) as u64);
                    if self.verbose {
                        tracing::info!(
                            workflow_id = %execution.workflow_id,
                            steps = step,
                            "Workflow completed successfully"
                        );
                    }
                    return Ok(output);
                }
                Err(e) if e.is_suspend() => e,
                Err(e) => {
                    self.elapsed = Duration::from_millis((now_ms - start_ms).max(0) as u64);
                    if self.verbose {
                        tracing::error!(
                            workflow_id = %execution.workflow_id,
                            error = %e,
                            step,
                            "Workflow execution failed"
                        );
                    }
                    return Err(e);
                }
            };

            // Resolve what the workflow scheduled. Tasks, child workflows and recorded
            // steps finish at once; timers are only noted, to fire when nothing else
            // can happen sooner.
            let mut progressed = false;
            // Timers this run is still waiting on. Only these can fire next: a timer
            // an earlier run started but this one no longer waits on (a deadline
            // whose event came first) is left alone, so it cannot skew the clock.
            let mut waiting_on: BTreeMap<String, i64> = BTreeMap::new();
            for command in commands_from.take_commands_for_test() {
                match command {
                    WorkflowCommand::ScheduleTask(task) => {
                        if resolved.insert(task.task_id.clone()) {
                            let result = self.run_task_mock(&task.task_type, &task.input)?;
                            journal.push(Entry::Task {
                                id: task.task_id,
                                result,
                                at: now_ms,
                            });
                            progressed = true;
                        }
                    }
                    WorkflowCommand::StartChildWorkflow(child) => {
                        let key = format!("child:{}", child.workflow_id);
                        if resolved.insert(key) {
                            let result = self.run_child_mock(&child.workflow_type, &child.input)?;
                            journal.push(Entry::Child {
                                id: child.workflow_id,
                                result,
                                at: now_ms,
                            });
                            progressed = true;
                        }
                    }
                    WorkflowCommand::RecordStepResult(step) => {
                        if resolved.insert(step.step_name.clone()) {
                            let result = match &step.failure {
                                Some(failure) => crate::workflow::executable::encode_task_failure(
                                    &failure.message,
                                    &failure.failure_type,
                                    step.execution_attempt,
                                ),
                                None => step.result,
                            };
                            journal.push(Entry::Step {
                                name: step.step_name,
                                result,
                                at: now_ms,
                            });
                            progressed = true;
                        }
                    }
                    WorkflowCommand::StartTimer(timer) => {
                        if !resolved.contains(&format!("timer:{}", timer.timer_id)) {
                            let due = *timer_due
                                .entry(timer.timer_id.clone())
                                .or_insert(now_ms + timer.duration.as_millis() as i64);
                            waiting_on.insert(timer.timer_id, due);
                        }
                    }
                    WorkflowCommand::CancelTimer(cancel) => {
                        waiting_on.remove(&cancel.timer_id);
                    }
                    // A wait is answered by the events the test sends; an event sent
                    // to another workflow and a cancelled child (already finished
                    // here) need nothing.
                    WorkflowCommand::WaitForEvent(_)
                    | WorkflowCommand::SendEvent(_)
                    | WorkflowCommand::CancelChildWorkflow(_) => {}
                    // The workflow's own ending is its return value, not a command to
                    // resolve.
                    WorkflowCommand::CompleteWorkflow(_)
                    | WorkflowCommand::FailWorkflow(_)
                    | WorkflowCommand::RestartFresh(_) => {}
                }
            }
            if progressed {
                continue;
            }

            // Nothing finished: skip to whichever comes first, the next timer or the
            // next event.
            let next_timer = waiting_on
                .iter()
                .min_by_key(|(id, at)| (**at, (*id).clone()))
                .map(|(id, at)| (id.clone(), *at));
            let next_event_at = events.front().map(|(at, _, _)| *at);
            let timer_first = match (&next_timer, next_event_at) {
                (Some((_, at)), Some(event_at)) => *at < event_at,
                (Some(_), None) => true,
                (None, _) => false,
            };
            if timer_first {
                let (id, at) = next_timer.expect("timer_first implies a timer");
                resolved.insert(format!("timer:{id}"));
                now_ms = now_ms.max(at);
                journal.push(Entry::Timer { id, at });
            } else if let Some((at, name, payload)) = events.pop_front() {
                now_ms = now_ms.max(at);
                journal.push(Entry::Event { name, payload, at });
            } else {
                return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                    format!(
                        "Workflow is waiting with nothing left to deliver: {error}. \
                         Send the event it waits for with with_event or \
                         with_event_after."
                    ),
                )));
            }
        }

        Err(Error::Workflow(crate::error::WorkflowError::StateError(
            format!(
                "Workflow exceeded maximum steps ({}). This might indicate an infinite loop or you need to configure max_steps higher.",
                self.max_steps
            ),
        )))
    }

    /// Run a workflow that takes no context.
    ///
    /// Such a workflow cannot call tasks, so it runs once, directly, and no mocks
    /// are involved. `_execution` is ignored.
    pub async fn execute_simple_workflow<F, Fut, I, O>(
        &mut self,
        workflow_fn: F,
        _execution: WorkflowExecution,
        input: I,
    ) -> Result<O>
    where
        F: Fn(I) -> Fut,
        Fut: std::future::Future<Output = Result<O>>,
        I: Clone + Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    {
        workflow_fn(input).await
    }

    /// Build a context holding everything in `journal`, fed in as the worker
    /// runtime feeds an activation: each entry at its place in journal order, with
    /// the time it was recorded, so the workflow's clock moves when it reads it.
    fn context_from_journal(
        execution: &WorkflowExecution,
        start_ms: i64,
        journal: &[Entry],
    ) -> WorkflowContext {
        let ctx = WorkflowContext::starting_at(
            execution.clone(),
            !journal.is_empty(),
            "1.0.0".to_string(),
            start_ms,
        );
        for (position, entry) in journal.iter().enumerate() {
            match entry {
                Entry::Task { id, result, at } => {
                    ctx.inject_task_result_for_test(id.clone(), result.clone());
                    ctx.record_resolved_at(id.clone(), *at);
                }
                Entry::Step { name, result, at } => {
                    ctx.inject_step_result(name.clone(), result.clone());
                    ctx.record_resolved_at(name.clone(), *at);
                }
                Entry::Child { id, result, at } => {
                    let key = format!("child:{id}");
                    ctx.inject_step_result(key.clone(), result.clone());
                    ctx.record_resolved_at(key, *at);
                }
                Entry::Timer { id, at } => {
                    ctx.mark_timer_fired_at(id.clone(), Some(position));
                    ctx.record_resolved_at(format!("timer:{id}"), *at);
                }
                Entry::Event { name, payload, at } => {
                    ctx.buffer_journaled_event(
                        name.clone(),
                        payload.clone(),
                        Some(position),
                        Some(*at),
                    );
                }
            }
        }
        ctx
    }

    /// Run the mock registered for a scheduled task and return its result as JSON
    /// bytes, or the journaled form of the failure if the mock fails the task.
    fn run_task_mock(&self, task_type: &str, input: &[u8]) -> Result<Vec<u8>> {
        let outcome = run_mock(&self.mocks, task_type, input).ok_or_else(|| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "No mock registered for task '{task_type}'. Use \
                 MockTaskBuilder::new(\"{task_type}\", ...).returns(...) to register one."
            )))
        })??;
        Ok(match outcome {
            Ok(result) => result,
            // The engine journals a task that fails for good, and the workflow sees it
            // as `TaskFailed`. The executor does not retry, so the task ran once.
            Err(message) => {
                crate::workflow::executable::encode_task_failure(&message, "MockFailure", 1)
            }
        })
    }

    /// Run the mock registered for a child workflow type and return the child's
    /// result as JSON bytes, or the stored form of its failure.
    fn run_child_mock(&self, workflow_type: &str, input: &[u8]) -> Result<Vec<u8>> {
        let outcome = run_mock(&self.child_mocks, workflow_type, input).ok_or_else(|| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "No mock registered for child workflow '{workflow_type}'. Register one \
                 in the registry passed to TestWorkflowExecutor::with_child_mocks."
            )))
        })??;
        Ok(match outcome {
            Ok(result) => result,
            // A failed child reaches its parent as `ChildWorkflowFailed`.
            Err(message) => crate::workflow::handle::encode_child_failure(&message),
        })
    }
}

/// Runs the mock for `name` in `registry` on `input`.
///
/// `None` if no mock is registered. `Some(Err)` if the mock's input or result does
/// not serialize: the test itself is wrong. Otherwise `Some(Ok)` with the result
/// bytes, or with the message of a mock that failed (`fails_with`, an exhausted
/// sequence, or a function mock returning an error).
#[allow(clippy::type_complexity)]
fn run_mock(
    registry: &RwLock<MockRegistry>,
    name: &str,
    input: &[u8],
) -> Option<Result<std::result::Result<Vec<u8>, String>>> {
    let mut registry = registry.write().unwrap();
    if !registry.has_mock(name) {
        return None;
    }
    // The executor does not know the types, so input and output pass through
    // `serde_json::Value`.
    let input: serde_json::Value = match serde_json::from_slice(input) {
        Ok(value) => value,
        Err(e) => {
            return Some(Err(Error::Serialization(format!(
                "Failed to deserialize input for '{name}': {e}"
            ))))
        }
    };
    Some(
        match registry.execute_mock::<_, serde_json::Value>(name, input) {
            Ok(value) => serde_json::to_vec(&value).map(Ok).map_err(|e| {
                Error::Serialization(format!("Failed to serialize result of '{name}': {e}"))
            }),
            Err(e @ Error::Serialization(_)) => Err(e),
            Err(Error::Workflow(crate::error::WorkflowError::StateError(message))) => {
                Ok(Err(message))
            }
            Err(other) => Ok(Err(other.to_string())),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks);
        assert_eq!(executor.max_steps, 1000);
        assert!(!executor.verbose);
    }

    #[test]
    fn test_executor_with_max_steps() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks).with_max_steps(500);
        assert_eq!(executor.max_steps, 500);
    }

    #[test]
    fn test_executor_with_verbose() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks).with_verbose(true);
        assert!(executor.verbose);
    }

    #[tokio::test]
    async fn test_simple_workflow_execution() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let mut executor = TestWorkflowExecutor::new(mocks);

        let execution = WorkflowExecution {
            workflow_id: "test-wf-1".to_string(),
            run_id: "test-run-1".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let workflow_fn = |input: i32| async move { Ok(input * 2) };
        let result: i32 = executor
            .execute_simple_workflow(workflow_fn, execution, 21)
            .await
            .unwrap();

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_context_workflow_without_tasks() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let mut executor = TestWorkflowExecutor::new(mocks);

        let execution = WorkflowExecution {
            workflow_id: "test-wf-2".to_string(),
            run_id: "test-run-2".to_string(),
            workflow_type: "ContextWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let workflow_fn = |ctx: WorkflowContext, input: String| async move {
            let wf_id = ctx.workflow_id();
            Ok(format!("{}-{}", wf_id, input))
        };
        let result: String = executor
            .execute_workflow(workflow_fn, execution.clone(), "hello".to_string())
            .await
            .unwrap();

        assert_eq!(result, format!("{}-hello", execution.workflow_id));
    }

    #[tokio::test]
    async fn test_context_cloning() {
        let execution = WorkflowExecution {
            workflow_id: "test-wf-3".to_string(),
            run_id: "test-run-3".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0.0".to_string());
        let ctx_clone = ctx.clone();

        assert_eq!(ctx.workflow_id(), ctx_clone.workflow_id());
        assert_eq!(ctx.workflow_type(), ctx_clone.workflow_type());
        assert_eq!(ctx.is_replaying(), ctx_clone.is_replaying());
    }
}
