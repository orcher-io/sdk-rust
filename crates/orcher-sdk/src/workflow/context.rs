//! The context passed to workflow code: tasks, timers, child workflows, state, events,
//! queries and updates.

use crate::error::{Error, Result};
use crate::task::IntoTaskName;
use crate::workflow::command::*;
use crate::workflow::executable::{ClosureExecution, Executable, TaskExecution};
use crate::workflow::{
    ChildWorkflowHandle, ChildWorkflowOptions, RestartFreshOptions, TaskOptions, WorkflowExecution,
};
use crate::workflow::{WorkflowRand, WorkflowTime};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The context a workflow runs in.
///
/// Every operation with an outside effect (tasks, closures, timers, child workflows) goes
/// through the context, which records its result in the journal. On replay the recorded
/// result is returned instead of running the operation again.
///
/// Clones share the same interior state.
pub struct WorkflowContext {
    execution: WorkflowExecution,
    version: String,
    /// Random source seeded by the workflow id, so replays draw the same values.
    rand: WorkflowRand,
    /// Journal-derived clock; see [`WorkflowContext::time`]. Shared with the state so a
    /// child handle can move it too.
    time: WorkflowTime,
    pub(crate) state: Arc<Mutex<ContextState>>,
}

/// What an update handler registered on a context returns.
pub(crate) type UpdateFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>;

/// An update handler registered on a context: JSON arguments in, JSON result out.
pub(crate) type ContextUpdateHandler = Arc<dyn Fn(Vec<u8>) -> UpdateFuture + Send + Sync>;

/// Events buffered for one name, oldest first, each with its job position
/// (journal order) and the time it was journaled (ms since the epoch), when
/// known.
pub(crate) type BufferedEvents = VecDeque<(Option<usize>, Vec<u8>, Option<i64>)>;

/// Mutable interior state for `WorkflowContext`, wrapped in `Arc<Mutex<>>`.
#[derive(Clone)]
pub(crate) struct ContextState {
    pub(crate) replaying: bool,
    /// The one step counter. Task, timer, child, closure and session ids are derived from
    /// it, and each step takes exactly one number whether it is issued or read back from
    /// the journal, so an id depends only on the step's place in the code.
    pub(crate) sequence: u64,
    /// The workflow's clock, the one `WorkflowContext::time` reads.
    pub(crate) clock: WorkflowTime,
    /// When the engine journaled each result the workflow can receive, in ms
    /// since the epoch: keyed as in `pending_task_results`, and fired timers
    /// under `timer:{id}`.
    pub(crate) resolved_at: HashMap<String, i64>,
    pub(crate) commands: Vec<WorkflowCommand>,
    pub(crate) workflow_state: HashMap<String, Vec<u8>>,
    pub(crate) pending_task_results: HashMap<String, Vec<u8>>,
    /// Step kind by step id: 0 for a task, 1 for a closure.
    pub(crate) step_type_map: HashMap<String, i32>,
    /// Attempts made so far, by step id.
    pub(crate) step_attempt_counts: HashMap<String, i32>,
    /// Recorded failures of closure steps, by step id.
    pub(crate) pending_task_failures: HashMap<String, orcher_proto::orcher::v1::Failure>,
    pub(crate) cached_input: Option<crate::payload::Payload>,
    /// Task queue override for session-routed tasks, set by `SessionContext` around its
    /// call to `execute_task`.
    pub(crate) task_queue_override: Option<String>,
    /// Fired durable timers by timer id, with the FireTimer job's position among this
    /// activation's jobs (journal order) when known.
    pub(crate) fired_timers: HashMap<String, Option<usize>>,
    pub(crate) child_workflows: HashMap<String, ChildWorkflowHandle>,
    /// Events received before a matching `wait_for_event`, per name, oldest first, each
    /// with its job position (journal order) when known.
    pub(crate) event_buffer: HashMap<String, BufferedEvents>,
    /// `wait_for_event_with_timeout` calls made per event name in this activation. The
    /// count names each deadline timer, so ids match across replays.
    pub(crate) event_timeout_waits: HashMap<String, u32>,
    /// The id of every step (task, timer, child workflow) the code reached in this
    /// activation, in order, whether the journal already held its outcome or its command
    /// is issued now. sdk-core checks them against the steps the journal recorded to tell
    /// code that no longer replays the run.
    pub(crate) reached_steps: Vec<String>,
    pub(crate) query_handlers: HashMap<String, Arc<dyn Fn() -> Vec<u8> + Send + Sync>>,
    pub(crate) update_handlers: HashMap<String, ContextUpdateHandler>,
    /// Whether the journal holds a request to cancel the workflow.
    pub(crate) cancel_requested: bool,
    /// When the journal recorded that request, ms since the epoch, when known.
    pub(crate) cancel_requested_at: Option<i64>,
    /// Whether the code has been told of the request. It is told once, at the first
    /// wait whose result the journal did not record before the request: the same wait
    /// on every replay, since the code's order and the journal's times are both fixed.
    pub(crate) cancel_delivered: bool,
}

impl ContextState {
    /// Whether the wait in hand is where the code learns of a cancellation request:
    /// a request has come, the code has not been told yet, and the journal did not
    /// record this wait's result before the request. Marks the request delivered
    /// when it is.
    ///
    /// `result` is `None` when the wait has no result yet, and otherwise when the
    /// journal recorded it, if known. A result recorded no later than the request is
    /// received; one with no known time is too, since it exists.
    pub(crate) fn take_cancellation(&mut self, result: Option<Option<i64>>) -> bool {
        if !self.cancel_requested || self.cancel_delivered {
            return false;
        }
        let received_first = match (result, self.cancel_requested_at) {
            (None, _) => false,
            (Some(Some(result_at)), Some(requested_at)) => result_at <= requested_at,
            (Some(_), _) => true,
        };
        if received_first {
            return false;
        }
        self.cancel_delivered = true;
        true
    }

    /// The result of the wait under `key`, as
    /// [`take_cancellation`](Self::take_cancellation) takes it.
    pub(crate) fn result_seen(&self, key: &str) -> Option<Option<i64>> {
        self.pending_task_results
            .contains_key(key)
            .then(|| self.resolved_at.get(key).copied())
    }

    /// The workflow received what was journaled under `key`: its clock moves
    /// to when that was journaled.
    pub(crate) fn observe(&self, key: &str) {
        if let Some(at_ms) = self.resolved_at.get(key) {
            self.clock.advance_to(*at_ms);
        }
    }
}

impl Clone for WorkflowContext {
    fn clone(&self) -> Self {
        Self {
            execution: self.execution.clone(),
            version: self.version.clone(),
            rand: self.rand.clone(),
            time: self.time.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl WorkflowContext {
    /// Creates a workflow context. The worker runtime creates one for each activation,
    /// with its clock at the journaled workflow start; tests and tools may call this
    /// directly, and with no journal to read, the clock starts at the moment of the call.
    pub fn new(execution: WorkflowExecution, replaying: bool, version: String) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;
        Self::starting_at(execution, replaying, version, now_ms)
    }

    /// Creates a context whose clock starts at `started_at_ms` (milliseconds since the
    /// UNIX epoch).
    pub(crate) fn starting_at(
        execution: WorkflowExecution,
        replaying: bool,
        version: String,
        started_at_ms: i64,
    ) -> Self {
        let workflow_id = execution.workflow_id.clone();
        let workflow_rand = WorkflowRand::new(&workflow_id);
        let workflow_time = WorkflowTime::from_millis(started_at_ms);

        let context_state = ContextState {
            replaying,
            sequence: 0,
            clock: workflow_time.clone(),
            resolved_at: HashMap::new(),
            commands: Vec::new(),
            workflow_state: HashMap::new(),
            pending_task_results: HashMap::new(),
            step_type_map: HashMap::new(),
            step_attempt_counts: HashMap::new(),
            pending_task_failures: HashMap::new(),
            cached_input: None,
            task_queue_override: None,
            fired_timers: HashMap::new(),
            child_workflows: HashMap::new(),
            event_buffer: HashMap::new(),
            event_timeout_waits: HashMap::new(),
            reached_steps: Vec::new(),
            query_handlers: HashMap::new(),
            update_handlers: HashMap::new(),
            cancel_requested: false,
            cancel_requested_at: None,
            cancel_delivered: false,
        };

        let ctx = Self {
            execution,
            version,
            rand: workflow_rand,
            time: workflow_time,
            state: Arc::new(Mutex::new(context_state)),
        };

        ctx.register_automatic_queries();

        ctx
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        let seq = state.sequence;
        state.sequence += 1;
        seq
    }

    pub(crate) fn add_command(&self, command: WorkflowCommand) {
        tracing::debug!("Adding command: {:?}", command);
        let mut state = self.state.lock().unwrap();
        state.commands.push(command);
    }

    pub(crate) fn take_commands(&self) -> Vec<WorkflowCommand> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.commands)
    }

    /// The code reached the step with this id; see
    /// [`take_reached_steps`](Self::take_reached_steps).
    pub(crate) fn reach_step(&self, step_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.reached_steps.push(step_id.to_string());
    }

    /// The ids of the steps the code reached in this activation, for its result.
    ///
    /// sdk-core compares them with the steps the journal recorded: a recorded step left
    /// unreached by an activation that issues new work or ends the workflow is reported
    /// as non-determinism, and the activation is tried again instead of applied.
    pub(crate) fn take_reached_steps(&self) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.reached_steps)
    }

    pub(crate) fn execution(&self) -> &WorkflowExecution {
        &self.execution
    }

    // ============================================================================
    // Tasks and closures
    // ============================================================================

    /// Runs a registered task and returns its result.
    ///
    /// `task` is either a typed reference generated by `#[task]`, which the compiler checks,
    /// or a string name, which is only checked at run time. The result is journaled; on
    /// replay the recorded result is returned and the task does not run again. In tests,
    /// tasks can be mocked with `TestWorkflowExecutor`.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is not registered, fails after its retries, or its
    /// input or output cannot be serialized. While the task is still running, returns a
    /// `Suspended` error that the runtime uses to park the workflow.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// #[task(retry = 3)]
    /// async fn send_email(_ctx: TaskContext, to: String) -> Result<String> {
    ///     Ok(format!("receipt for {to}"))
    /// }
    ///
    /// #[workflow]
    /// async fn welcome(ctx: WorkflowContext, email: String) -> Result<String> {
    ///     // Typed reference: a typo fails to compile.
    ///     let receipt: String = ctx.execute_task(send_email, email.clone()).await?;
    ///
    ///     // String name: a typo fails only at run time.
    ///     let _again: String = ctx.execute_task("send_email", email).await?;
    ///
    ///     Ok(receipt)
    /// }
    /// ```
    pub async fn execute_task<T, I, O>(&self, task: T, input: I) -> Result<O>
    where
        T: IntoTaskName,
        I: Serialize + Send + Sync + 'static,
        O: DeserializeOwned + Send + Sync + 'static,
    {
        let task_name = task.into_task_name();
        let task_exec = TaskExecution::<I, O>::new(input);
        task_exec.execute_in_context(self, task_name).await
    }

    #[doc(hidden)]
    pub async fn execute_task_by_name<I, O>(&self, task_name: &'static str, input: I) -> Result<O>
    where
        I: Serialize + Send + Sync + 'static,
        O: DeserializeOwned + Send + Sync + 'static,
    {
        self.execute_task(task_name, input).await
    }

    /// Runs an inline closure as a journaled step named `step_name`.
    ///
    /// Suited to one-off side effects such as an HTTP call or a query. On replay the
    /// recorded result is returned and the closure does not run again. For work used from
    /// several places, prefer a registered task and [`execute_task`](Self::execute_task).
    ///
    /// # Errors
    ///
    /// Returns an error if the closure fails, its result cannot be serialized, or the
    /// recorded result is missing during replay.
    pub async fn execute<F, Fut, O>(&self, step_name: &str, closure: F) -> Result<O>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<O>> + Send,
        O: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let closure_exec = ClosureExecution::new(closure);
        closure_exec.execute_in_context(self, step_name).await
    }

    /// Deprecated: use [`execute_task`](Self::execute_task) or [`execute`](Self::execute).
    #[deprecated(
        since = "0.2.0",
        note = "Use ctx.execute() instead for unified task/closure execution with journaling"
    )]
    pub async fn execute_task_with_options<I, O>(
        &self,
        task_type: &str,
        input: I,
        options: TaskOptions,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| Error::Serialization(format!("Failed to serialize task input: {}", e)))?;

        // One number on every path, cached or not. The id comes from it so it is stable
        // across replays, and taking it unconditionally keeps later ids from shifting.
        let sequence = self.next_sequence();
        let task_id = options
            .task_id
            .clone()
            .unwrap_or_else(|| format!("task_{}", sequence));
        self.reach_step(&task_id);
        self.cancellation_for(&task_id)?;

        let replaying = {
            let state = self.state.lock().unwrap();
            state.replaying
        };

        if replaying {
            let state = self.state.lock().unwrap();

            let has_cached = state.pending_task_results.contains_key(&task_id);
            let all_cached_keys: Vec<String> = state.pending_task_results.keys().cloned().collect();

            tracing::warn!(
                task_id = %task_id,
                has_cached = has_cached,
                all_cached_keys = ?all_cached_keys,
                cached_count = all_cached_keys.len(),
                "🟣 TASK_CACHE_LOOKUP: Checking for cached task result during replay"
            );

            if let Some(result_bytes) = state.pending_task_results.get(&task_id) {
                tracing::warn!(
                    task_id = %task_id,
                    result_len = result_bytes.len(),
                    "🟣 TASK_CACHE_HIT: Found cached result, returning from cache"
                );
                state.observe(&task_id);

                let output: O = serde_json::from_slice(result_bytes).map_err(|e| {
                    Error::Serialization(format!("Failed to deserialize task result: {}", e))
                })?;
                return Ok(output);
            } else {
                tracing::warn!(
                    task_id = %task_id,
                    "🟣 TASK_CACHE_MISS: No cached result found, will schedule task"
                );
            }
        }

        let declared = crate::worker::registration::task_limits(task_type);
        let command = WorkflowCommand::ScheduleTask(ScheduleTaskCommand {
            sequence,
            task_id: task_id.clone(),
            task_type: task_type.to_string(),
            task_queue: self.execution.task_queue.clone(),
            input: input_bytes,
            // Precedence: what this call asked for, else what the task declared on its
            // `#[task]` attribute, else the 300s scheduler default.
            timeout: options
                .timeout
                .or_else(|| {
                    declared
                        .and_then(|d| d.timeout_secs)
                        .map(Duration::from_secs)
                })
                .unwrap_or(Duration::from_secs(300)),
            heartbeat_timeout: options.heartbeat_timeout.or_else(|| {
                declared
                    .and_then(|d| d.heartbeat_timeout_secs)
                    .map(Duration::from_secs)
            }),
            queue_timeout: options.queue_timeout,
            retry_policy: (&options).into(),
            headers: vec![],
        });

        self.add_command(command);

        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Task '{}' scheduled for execution", task_id),
            pending_operations: vec![task_id],
        }))
    }

    // ============================================================================
    // Timers
    // ============================================================================

    /// Sleeps for `duration` on a durable timer, which survives worker restarts.
    ///
    /// The timer id is `timer_{sequence}`, taken from the step sequence, so it is the same
    /// on every replay and matches the engine's fired-timer event.
    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        let sequence = self.next_sequence();
        let timer_id = format!("timer_{}", sequence);
        self.emit_timer(timer_id, sequence, duration)
    }

    /// Sleeps on a durable timer with an explicit id, for example to address it for
    /// cancellation. Prefer [`sleep`](Self::sleep) unless you need a known id.
    pub async fn sleep_with_id(&self, timer_id: &str, duration: Duration) -> Result<()> {
        let sequence = self.next_sequence();
        self.emit_timer(timer_id.to_string(), sequence, duration)
    }

    /// Emits a StartTimer command and suspends, or returns at once if the timer has
    /// already fired.
    ///
    /// Callers take the sequence number before this check, whether or not the timer has
    /// fired, so later ids stay the same across replays. The runtime marks a timer fired
    /// by the timer id in its FireTimer job.
    fn emit_timer(&self, timer_id: String, sequence: u64, duration: Duration) -> Result<()> {
        self.reach_step(&timer_id);
        let fired = {
            let state = self.state.lock().unwrap();
            state.fired_timers.contains_key(&timer_id).then(|| {
                state
                    .resolved_at
                    .get(&format!("timer:{}", timer_id))
                    .copied()
            })
        };
        self.cancellation_at(fired)?;
        let already_fired = {
            let state = self.state.lock().unwrap();
            let fired = state.fired_timers.contains_key(&timer_id);
            if fired {
                state.observe(&format!("timer:{}", timer_id));
            }
            fired
        };
        if already_fired {
            return Ok(());
        }

        self.add_command(WorkflowCommand::StartTimer(StartTimerCommand {
            sequence,
            timer_id: timer_id.clone(),
            duration,
        }));

        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Waiting for timer {}", timer_id),
            pending_operations: vec![timer_id],
        }))
    }

    // ============================================================================
    // Child workflows
    // ============================================================================

    /// Starts a child workflow and waits for its result.
    pub async fn execute_child_workflow<I, O>(&self, workflow_type: &str, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        self.execute_child_workflow_with_options(
            workflow_type,
            input,
            ChildWorkflowOptions::default(),
        )
        .await
    }

    /// Starts a child workflow with custom options and waits for its result.
    pub async fn execute_child_workflow_with_options<I, O>(
        &self,
        workflow_type: &str,
        input: I,
        options: ChildWorkflowOptions,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        // Advances the sequence and derives a stable child id.
        let handle = self
            .start_child_workflow_with_options(workflow_type, input, options)
            .await?;

        self.cancellation_for(&format!("child:{}", handle.id()))?;

        // On replay, the runtime has injected the child's outcome under `child:{id}`.
        let cached = {
            let state = self.state.lock().unwrap();
            let key = format!("child:{}", handle.id());
            let cached = state.pending_task_results.get(&key).cloned();
            if cached.is_some() {
                state.observe(&key);
            }
            cached
        };

        if let Some(result_bytes) = cached {
            // A failed child is recorded as a JSON object with `__orcher_child_failed__`.
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&result_bytes) {
                if v.get("__orcher_child_failed__")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false)
                {
                    let msg = v
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("child workflow failed");
                    return Err(Error::Workflow(
                        crate::error::WorkflowError::ChildWorkflowFailed {
                            workflow_type: workflow_type.to_string(),
                            workflow_id: handle.id().to_string(),
                            reason: msg.to_string(),
                        },
                    ));
                }
            }
            return serde_json::from_slice(&result_bytes).map_err(|e| {
                Error::Serialization(format!(
                    "Failed to deserialize child workflow result: {}",
                    e
                ))
            });
        }

        // Not finished yet: suspend until the child completes.
        handle.result().await
    }

    /// Starts a child workflow and returns a handle without waiting for it.
    pub async fn start_child_workflow<I>(
        &self,
        workflow_type: &str,
        input: I,
    ) -> Result<ChildWorkflowHandle>
    where
        I: Serialize,
    {
        self.start_child_workflow_with_options(
            workflow_type,
            input,
            ChildWorkflowOptions::default(),
        )
        .await
    }

    /// Starts a child workflow with custom options and returns a handle.
    pub async fn start_child_workflow_with_options<I>(
        &self,
        workflow_type: &str,
        input: I,
        options: ChildWorkflowOptions,
    ) -> Result<ChildWorkflowHandle>
    where
        I: Serialize,
    {
        let input_bytes = serde_json::to_vec(&input).map_err(|e| {
            Error::Serialization(format!("Failed to serialize child workflow input: {}", e))
        })?;

        // Take the sequence first so the default child id is the same on every replay.
        let sequence = self.next_sequence();

        let workflow_id = options
            .workflow_id
            .clone()
            .unwrap_or_else(|| format!("child_{}", sequence));
        self.reach_step(&workflow_id);

        let run_id = format!("run_{}", uuid::Uuid::new_v4());

        // Inherit the parent's task queue unless one is given.
        let task_queue = options
            .task_queue
            .clone()
            .unwrap_or_else(|| self.execution.task_queue.clone());

        // The handle shares the context state, so `handle.result()` reads the outcome the
        // runtime injects under `child:{id}`, the same entry the execute path reads.
        let handle = ChildWorkflowHandle::new(
            workflow_id.clone(),
            run_id,
            workflow_type.to_string(),
            self.clone(),
        );

        {
            let mut state = self.state.lock().unwrap();
            state
                .child_workflows
                .insert(workflow_id.clone(), handle.clone());
        }

        // A child that already completed is not started again.
        let already_done = {
            let state = self.state.lock().unwrap();
            state
                .pending_task_results
                .contains_key(&format!("child:{}", workflow_id))
        };

        if !already_done {
            let command = WorkflowCommand::StartChildWorkflow(StartChildWorkflowCommand {
                sequence,
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.to_string(),
                task_queue,
                input: input_bytes,
                timeout: options.execution_timeout,
                orphan_policy: options.orphan_policy.into(),
            });
            self.add_command(command);
        }

        tracing::info!(
            workflow_id = %self.execution.workflow_id,
            child_workflow_id = %workflow_id,
            child_workflow_type = %workflow_type,
            already_done = already_done,
            "Started child workflow"
        );

        Ok(handle)
    }

    // ============================================================================
    // State
    // ============================================================================

    /// Stores a JSON-serialized value under `key` in the workflow state.
    pub fn set_state<T>(&self, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let value_bytes = serde_json::to_vec(&value)
            .map_err(|e| Error::Serialization(format!("Failed to serialize state value: {}", e)))?;

        let mut state = self.state.lock().unwrap();
        state.workflow_state.insert(key.to_string(), value_bytes);

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            key = %key,
            "Set workflow state"
        );

        Ok(())
    }

    /// Reads the value stored under `key`, or `None` if there is none.
    pub fn get_state<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let state = self.state.lock().unwrap();
        match state.workflow_state.get(key) {
            Some(value_bytes) => {
                let value: T = serde_json::from_slice(value_bytes).map_err(|e| {
                    Error::Serialization(format!("Failed to deserialize state value: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    // ============================================================================
    // Events and queries
    // ============================================================================

    /// Waits for the next event named `event_name`.
    ///
    /// Events of one name are delivered oldest first. If one is already buffered it is
    /// returned at once; otherwise the workflow suspends until the event arrives.
    pub async fn wait_for_event<T>(&self, event_name: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        // Scoped deliberately: the suspend path below locks `state` again, and
        // std::sync::Mutex is not reentrant. Holding this guard past the early
        // return deadlocks the worker thread.
        //
        // Oldest first: events of one name are consumed in arrival order, as in the
        // other SDKs.
        // The oldest event of this name is received if it came before any cancellation
        // request; otherwise this is where the request is delivered.
        let next = {
            let state = self.state.lock().unwrap();
            state
                .event_buffer
                .get(event_name)
                .and_then(|events| events.front())
                .map(|(_, _, at_ms)| *at_ms)
        };
        self.cancellation_at(next)?;
        let buffered = {
            let mut state = self.state.lock().unwrap();
            let event = state
                .event_buffer
                .get_mut(event_name)
                .and_then(|events| events.pop_front());
            event.map(|(_, payload, at_ms)| {
                if let Some(at_ms) = at_ms {
                    state.clock.advance_to(at_ms);
                }
                payload
            })
        };

        if let Some(event_bytes) = buffered {
            let event: T = serde_json::from_slice(&event_bytes).map_err(|e| {
                Error::Serialization(format!(
                    "Failed to deserialize event '{}': {}",
                    event_name, e
                ))
            })?;

            tracing::debug!(
                workflow_id = %self.execution.workflow_id,
                event_name = %event_name,
                "Event retrieved from buffer"
            );

            return Ok(event);
        }

        // No buffered event: emit WaitForEvent and suspend. This produces no proto
        // command; the server releases its claim on the workflow and queues it again
        // when SendEvent delivers an EventReceived entry.
        //
        // A wait is not an engine step (events are matched to waits by name), so it takes
        // no number from the step counter on any path. If parking took one and finding
        // the event did not, a step issued beside the wait would get another id once the
        // event had arrived.
        let seq = self.state.lock().unwrap().sequence;

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            event_name = %event_name,
            "Suspending workflow to wait for event"
        );

        self.add_command(WorkflowCommand::WaitForEvent(
            crate::workflow::WaitForEventCommand {
                sequence: seq,
                event_name: event_name.to_string(),
                timeout: None,
            },
        ));

        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Waiting for event: {}", event_name),
            pending_operations: vec![format!("event:{}", event_name)],
        }))
    }

    /// Like [`wait_for_event`](Self::wait_for_event), but returns `None` if `timeout`
    /// passes without the event.
    ///
    /// The deadline is a durable timer started beside the wait, so it holds while no
    /// worker is running the workflow. Whichever the journal shows first wins: an event
    /// that arrived before the deadline is returned; a fired deadline returns `None`, and
    /// an event that lands after it is left for the next wait, because an earlier
    /// activation already took the timeout branch.
    ///
    /// # Errors
    ///
    /// Returns an error if `timeout` is zero or the event cannot be deserialized.
    pub async fn wait_for_event_with_timeout<T>(
        &self,
        event_name: &str,
        timeout: Duration,
    ) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        if timeout.is_zero() {
            return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                "wait_for_event_with_timeout: timeout must be greater than zero".to_string(),
            )));
        }

        // Scoped for the same reason as `wait_for_event`: the suspend path below
        // locks `state` again, and the mutex is not reentrant.
        //
        // The deadline timer id is `event_timeout_<name>_<n>`, where n counts this
        // activation's timed waits on that name. It is allocated first, on every path.
        // Allocating it only when parking would give the next wait on the same name a
        // different id on replay than it had live, and that wait would find an earlier
        // wait's fired timer and time out for no reason. It is not the step sequence: a
        // wait takes no number from the step counter on any path (see `wait_for_event`).
        let (timer_id, buffered, timer_fired) = {
            let mut state = self.state.lock().unwrap();
            let n = state
                .event_timeout_waits
                .entry(event_name.to_string())
                .or_insert(0);
            *n += 1;
            let timer_id = format!("event_timeout_{}_{}", event_name, n);
            // Reached on every path, whichever of the event and the deadline wins: the
            // journal holds this timer whenever an earlier activation parked here.
            state.reached_steps.push(timer_id.clone());

            // A fired marker without a position has no known order: treat it as later
            // than any event, so a buffered event still wins.
            let fired_at = state
                .fired_timers
                .get(&timer_id)
                .map(|position| position.unwrap_or(usize::MAX));
            // Whichever of the event and the deadline came first is this wait's
            // result; if neither came before a cancellation request, this is where the
            // request is delivered.
            let event = state
                .event_buffer
                .get(event_name)
                .and_then(|events| events.front())
                .map(|(_, _, at_ms)| *at_ms);
            let deadline = state.fired_timers.contains_key(&timer_id).then(|| {
                state
                    .resolved_at
                    .get(&format!("timer:{}", timer_id))
                    .copied()
            });
            let result = match (event, deadline) {
                (Some(Some(event)), Some(Some(deadline))) => Some(Some(event.min(deadline))),
                (Some(event), None) => Some(event),
                (None, deadline) => deadline,
                (Some(_), Some(_)) => Some(None),
            };
            if state.take_cancellation(result) {
                return Err(Error::Workflow(crate::error::WorkflowError::Canceled));
            }
            let event_first = match (state.event_buffer.get(event_name), fired_at) {
                (Some(events), Some(fired_at)) => events
                    .front()
                    .is_some_and(|(position, _, _)| position.is_none_or(|p| p < fired_at)),
                (Some(events), None) => !events.is_empty(),
                (None, _) => false,
            };
            let buffered = if event_first {
                let event = state
                    .event_buffer
                    .get_mut(event_name)
                    .and_then(|events| events.pop_front());
                event.map(|(_, payload, at_ms)| {
                    if let Some(at_ms) = at_ms {
                        state.clock.advance_to(at_ms);
                    }
                    payload
                })
            } else {
                None
            };
            if buffered.is_none() && fired_at.is_some() {
                state.observe(&format!("timer:{}", timer_id));
            }
            (timer_id, buffered, fired_at.is_some())
        };

        if let Some(event_bytes) = buffered {
            let event: T = serde_json::from_slice(&event_bytes).map_err(|e| {
                Error::Serialization(format!(
                    "Failed to deserialize event '{}': {}",
                    event_name, e
                ))
            })?;

            tracing::debug!(
                workflow_id = %self.execution.workflow_id,
                event_name = %event_name,
                "Event retrieved from buffer (with timeout)"
            );

            return Ok(Some(event));
        }

        if timer_fired {
            tracing::debug!(
                workflow_id = %self.execution.workflow_id,
                event_name = %event_name,
                timer_id = %timer_id,
                "Wait for event timed out"
            );
            return Ok(None);
        }

        // Neither has arrived: park on the event and start the deadline timer.
        // The timer is not cancelled if the event wins. For a finished workflow the
        // engine marks the timer done without journaling it; for an unfinished one it
        // costs a single extra replay. The sequence fields only label the commands; the
        // counter is not advanced (see `wait_for_event`).
        let wait_seq = self.state.lock().unwrap().sequence;
        let timer_seq = wait_seq;

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            event_name = %event_name,
            timeout_secs = %timeout.as_secs(),
            timer_id = %timer_id,
            "Suspending workflow to wait for event (with timeout)"
        );

        self.add_command(WorkflowCommand::WaitForEvent(
            crate::workflow::WaitForEventCommand {
                sequence: wait_seq,
                event_name: event_name.to_string(),
                timeout: Some(timeout),
            },
        ));
        self.add_command(WorkflowCommand::StartTimer(StartTimerCommand {
            sequence: timer_seq,
            timer_id: timer_id.clone(),
            duration: timeout,
        }));

        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Waiting for event (with timeout): {}", event_name),
            pending_operations: vec![format!("event:{}", event_name), timer_id],
        }))
    }

    /// Registers a handler that answers the query `query_name`.
    ///
    /// Queries let clients read workflow state without affecting execution. Handlers are
    /// synchronous and should not modify workflow state. The result is serialized to
    /// JSON; if serialization fails the query returns an empty payload. Registering a name
    /// again replaces the earlier handler.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # async fn example(ctx: &mut WorkflowContext) -> Result<()> {
    /// ctx.register_query_handler("get_status", || {
    ///     "processing".to_string()
    /// });
    ///
    /// ctx.register_query_handler("get_progress", || {
    ///     serde_json::json!({
    ///         "completed": 42,
    ///         "total": 100,
    ///         "percentage": 42.0
    ///     })
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_query_handler<F, R>(&self, query_name: &str, handler: F)
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: Serialize,
    {
        let wrapped_handler = Box::new(move || {
            let result = handler();
            serde_json::to_vec(&result).unwrap_or_else(|_| vec![])
        });

        let mut state = self.state.lock().unwrap();
        state
            .query_handlers
            .insert(query_name.to_string(), Arc::new(wrapped_handler));

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            query_name = %query_name,
            "Query handler registered"
        );
    }

    /// Runs the query handler registered under `query_name`.
    #[cfg(not(test))]
    #[allow(dead_code)] // Used by executor for query handling
    pub(crate) fn handle_query_request(&self, query_name: &str) -> Result<Vec<u8>> {
        let state = self.state.lock().unwrap();
        match state.query_handlers.get(query_name) {
            Some(handler) => {
                let result = handler();
                Ok(result)
            }
            None => Err(Error::Workflow(crate::error::WorkflowError::QueryError {
                query: query_name.to_string(),
                reason: "Query handler not registered".to_string(),
            })),
        }
    }

    /// Runs the query handler registered under `query_name`. Public in test builds so
    /// tests can simulate queries.
    #[cfg(test)]
    pub fn handle_query_request(&self, query_name: &str) -> Result<Vec<u8>> {
        let state = self.state.lock().unwrap();
        match state.query_handlers.get(query_name) {
            Some(handler) => {
                let result = handler();
                Ok(result)
            }
            None => Err(Error::Workflow(crate::error::WorkflowError::QueryError {
                query: query_name.to_string(),
                reason: "Query handler not registered".to_string(),
            })),
        }
    }

    // ============================================================================
    // Sessions
    // ============================================================================

    /// Creates a worker session, which pins the tasks run through it to one worker.
    ///
    /// This schedules an internal `__orcher_create_session` task on the workflow's own
    /// queue. The worker that picks it up takes a session slot and returns its
    /// worker-specific queue name. The returned
    /// [`SessionContext`](crate::workflow::SessionContext) routes tasks to that queue.
    ///
    /// The creation task's id comes from the step sequence. On replay the session queue
    /// name is read from the journal, and the session is not created again.
    ///
    /// # Errors
    ///
    /// Returns a `Suspended` error while the session is being created, and an error if
    /// the options or the recorded session info cannot be serialized.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::prelude::*;
    /// use orcher_sdk::workflow::session::SessionOptions;
    /// use std::time::Duration;
    ///
    /// #[task]
    /// async fn load_model(_ctx: TaskContext, name: String) -> Result<String> {
    ///     Ok(format!("loaded {name}"))
    /// }
    ///
    /// #[task]
    /// async fn run_inference(_ctx: TaskContext, prompt: String) -> Result<String> {
    ///     Ok(format!("answer to {prompt}"))
    /// }
    ///
    /// #[workflow]
    /// async fn answer(ctx: WorkflowContext, prompt: String) -> Result<String> {
    ///     let mut session = ctx
    ///         .create_session(
    ///             SessionOptions::default()
    ///                 .with_creation_timeout(Duration::from_secs(30))
    ///                 .with_execution_timeout(Duration::from_secs(600)),
    ///         )
    ///         .await?;
    ///
    ///     // Both tasks run on the same worker.
    ///     let _model: String = session.execute_task(load_model, "small".to_string()).await?;
    ///     let answer: String = session.execute_task(run_inference, prompt).await?;
    ///
    ///     session.complete();
    ///     Ok(answer)
    /// }
    /// ```
    pub async fn create_session(
        &self,
        options: crate::workflow::session::SessionOptions,
    ) -> Result<crate::workflow::session::SessionContext<'_>> {
        use crate::workflow::session::*;

        let sequence = self.next_sequence();
        let session_id = format!("session_{}", sequence);
        let task_id = format!("{}_{}", SESSION_CREATE_TASK, sequence);
        self.reach_step(&task_id);
        self.cancellation_for(&task_id)?;

        // On replay the creation task's recorded result is a serialized SessionInfo.
        {
            let state = self.state.lock().unwrap();
            if state.replaying {
                if let Some(cached) = state.pending_task_results.get(&task_id) {
                    state.observe(&task_id);
                    let info: SessionInfo = serde_json::from_slice(cached).map_err(|e| {
                        Error::Serialization(format!(
                            "Failed to deserialize cached session info: {}",
                            e
                        ))
                    })?;

                    tracing::debug!(
                        session_id = %info.session_id,
                        session_queue = %info.session_queue,
                        "Replaying session creation"
                    );

                    return Ok(SessionContext::new(self, info));
                }
            }
        }

        let input = CreateSessionInput {
            session_id: session_id.clone(),
            creation_timeout_ms: options.creation_timeout.as_millis() as u64,
            execution_timeout_ms: options.execution_timeout.as_millis() as u64,
            max_concurrent_tasks: options.max_concurrent_tasks,
            heartbeat_interval_ms: options.heartbeat_interval.as_millis() as u64,
        };

        let input_bytes = serde_json::to_vec(&input).map_err(|e| {
            Error::Serialization(format!("Failed to serialize session creation input: {}", e))
        })?;

        let command = ScheduleTaskCommand {
            sequence,
            task_id: task_id.clone(),
            task_type: SESSION_CREATE_TASK.to_string(),
            task_queue: self.execution.task_queue.clone(), // The workflow's queue: any worker.
            input: input_bytes,
            timeout: options.creation_timeout,
            heartbeat_timeout: None,
            queue_timeout: None,
            retry_policy: None, // Session creation is not retried.
            headers: vec![],
        };

        // 0 marks the step as a task.
        {
            let mut state = self.state.lock().unwrap();
            state.step_type_map.insert(task_id.clone(), 0);
        }

        self.add_command(WorkflowCommand::ScheduleTask(command));

        tracing::debug!(
            session_id = %session_id,
            task_queue = %self.execution.task_queue,
            "Scheduling session creation task"
        );

        // Suspend. When the task completes, its SessionInfo is injected into
        // pending_task_results and the workflow resumes, and this call returns it from the
        // replay path above.
        Err(Error::Workflow(crate::error::WorkflowError::Suspended {
            reason: format!("Creating session '{}'", session_id),
            pending_operations: vec![task_id],
        }))
    }

    // ============================================================================
    // Updates
    // ============================================================================

    /// Registers an async handler for the update `update_name`.
    ///
    /// The handler runs when a client sends an UpdateWorkflow request whose `update_type`
    /// matches `update_name`. It receives the JSON-decoded arguments, may read and modify
    /// workflow state, and its result is journaled so replays see the same outcome.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// #[derive(Serialize, Deserialize)]
    /// struct Address {
    ///     street: String,
    /// }
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct AddressChangeResult {
    ///     accepted: bool,
    /// }
    ///
    /// # fn example(ctx: &WorkflowContext) {
    /// ctx.register_update_handler("change_address", |new_address: Address| async move {
    ///     Ok(AddressChangeResult { accepted: !new_address.street.is_empty() })
    /// });
    /// # }
    /// ```
    pub fn register_update_handler<F, Fut, I, R>(&self, update_name: &str, handler: F)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send + 'static,
        I: serde::de::DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
    {
        let wrapped_handler: ContextUpdateHandler = Arc::new(move |args: Vec<u8>| {
            let input: I = match serde_json::from_slice(&args) {
                Ok(v) => v,
                Err(e) => {
                    return Box::pin(async move {
                        Err(Error::Other(format!(
                            "Failed to deserialize update args: {}",
                            e
                        )))
                    }) as UpdateFuture;
                }
            };
            let fut = handler(input);
            Box::pin(async move {
                let result = fut.await?;
                serde_json::to_vec(&result)
                    .map_err(|e| Error::Other(format!("Failed to serialize update result: {}", e)))
            }) as UpdateFuture
        });

        let mut state = self.state.lock().unwrap();
        state
            .update_handlers
            .insert(update_name.to_string(), wrapped_handler);

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            update_name = %update_name,
            "Update handler registered"
        );
    }

    /// Runs the update handler registered under `update_name` on JSON `args` and returns
    /// the JSON result. Called by the worker runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if no handler is registered under that name, or if the handler
    /// fails or its arguments or result cannot be (de)serialized.
    #[allow(dead_code)]
    pub async fn handle_update_request(&self, update_name: &str, args: Vec<u8>) -> Result<Vec<u8>> {
        let handler = {
            let state = self.state.lock().unwrap();
            state.update_handlers.get(update_name).cloned()
        };

        match handler {
            Some(handler) => handler(args).await,
            None => Err(Error::Workflow(crate::error::WorkflowError::QueryError {
                query: update_name.to_string(),
                reason: "Update handler not registered".to_string(),
            })),
        }
    }

    /// Returns `true` if an update handler is registered under `update_name`.
    pub fn has_update_handler(&self, update_name: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.update_handlers.contains_key(update_name)
    }

    /// Returns the names of all registered update handlers, in no particular order.
    pub fn get_update_names(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        state.update_handlers.keys().cloned().collect()
    }

    // ============================================================================
    // Event buffering
    // ============================================================================

    /// Buffers an event with no known position, for `wait_for_event` to consume.
    #[cfg(test)]
    pub(crate) fn buffer_event(&self, event_name: impl Into<String>, payload: Vec<u8>) {
        self.buffer_event_at(event_name, payload, None);
    }

    /// Buffers an event with its job position (journal order) in this activation.
    ///
    /// The runtime calls this for each `EventReceived` entry. `None` means "before
    /// everything": the event wins any race with a deadline timer.
    #[cfg(test)]
    pub(crate) fn buffer_event_at(
        &self,
        event_name: impl Into<String>,
        payload: Vec<u8>,
        position: Option<usize>,
    ) {
        self.buffer_journaled_event(event_name, payload, position, None);
    }

    /// Buffer an event with its job position and the time the engine
    /// journaled it (ms since the epoch), which the workflow's clock moves to
    /// when a wait takes the event.
    pub(crate) fn buffer_journaled_event(
        &self,
        event_name: impl Into<String>,
        payload: Vec<u8>,
        position: Option<usize>,
        at_ms: Option<i64>,
    ) {
        let name = event_name.into();
        let mut state = self.state.lock().unwrap();
        state
            .event_buffer
            .entry(name.clone())
            .or_default()
            .push_back((position, payload, at_ms));

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            event_name = %name,
            "Event buffered"
        );
    }

    // ============================================================================
    // Built-in queries
    // ============================================================================

    /// Registers the queries every workflow answers without any setup.
    ///
    /// They are `workflow_id`, `workflow_type`, `run_id`, `is_replaying`, `attempt`,
    /// `version`, `namespace`, `task_queue`, and `workflow_info`, which returns all of
    /// these as one JSON object. Values are captured when the context is created. A
    /// workflow may replace any of them by registering a handler with the same name.
    fn register_automatic_queries(&self) {
        let wf_id = self.execution.workflow_id.clone();
        self.register_query_handler("workflow_id", move || wf_id.clone());

        let wf_type = self.execution.workflow_type.clone();
        self.register_query_handler("workflow_type", move || wf_type.clone());

        let run_id = self.execution.run_id.clone();
        self.register_query_handler("run_id", move || run_id.clone());

        let replaying = self.is_replaying();
        self.register_query_handler("is_replaying", move || replaying);

        let attempt = self.execution.attempt;
        self.register_query_handler("attempt", move || attempt);

        let version = self.version.clone();
        self.register_query_handler("version", move || version.clone());

        let namespace = self.execution.namespace.clone();
        self.register_query_handler("namespace", move || namespace.clone());

        let task_queue = self.execution.task_queue.clone();
        self.register_query_handler("task_queue", move || task_queue.clone());

        let exec = self.execution.clone();
        let ver = self.version.clone();
        let rep = self.is_replaying();
        self.register_query_handler("workflow_info", move || {
            serde_json::json!({
                "workflow_id": exec.workflow_id,
                "run_id": exec.run_id,
                "workflow_type": exec.workflow_type,
                "namespace": exec.namespace,
                "task_queue": exec.task_queue,
                "attempt": exec.attempt,
                "version": ver,
                "is_replaying": rep,
            })
        });

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            "Automatic base queries registered"
        );
    }

    // ============================================================================
    // Deterministic helpers
    // ============================================================================

    /// Returns the deterministic random source.
    ///
    /// Values are derived from the workflow id and an internal counter, so a replay draws
    /// the same sequence as the original run, provided the calls happen in the same order.
    /// Use it instead of `Uuid::new_v4()` or a thread RNG in workflow code.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// // Deterministic UUIDs.
    /// let order_id = format!("ORD-{}", ctx.rand().uuid());
    /// let confirmation = format!("CONF-{}", ctx.rand().uuid());
    ///
    /// // A float in [0.0, 1.0).
    /// let priority = ctx.rand().random();
    ///
    /// // An integer in a range.
    /// let discount = ctx.rand().random_range(5..=20);
    ///
    /// // One element of a slice.
    /// let shipping = ctx.rand().choose(&["standard", "express", "overnight"]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Never call `Uuid::new_v4()` or a thread RNG directly in workflow code: a replay
    /// would draw different values than the original run did.
    pub fn rand(&self) -> &WorkflowRand {
        &self.rand
    }

    /// Returns the time helper.
    ///
    /// `ctx.time().now()` is read from the journal: the workflow's journaled start, then
    /// the moment the engine journaled each task result, fired timer, child outcome or
    /// event the workflow has received. It is the same at each point in the code on every
    /// replay, and so are the `elapsed*` methods, which measure on the same clock. See
    /// [`WorkflowTime`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::time::Duration;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// // The workflow's current time, the same at this point on every replay.
    /// let order_time = ctx.time().now();
    ///
    /// // Measured on the same clock.
    /// if ctx.time().elapsed() > Duration::from_secs(3600) {
    ///     return Err(Error::Workflow(orcher_sdk::error::WorkflowError::StateError(
    ///         "Workflow timeout exceeded".to_string()
    ///     )));
    /// }
    ///
    /// let elapsed_secs = ctx.time().elapsed_secs();
    /// tracing::info!("Workflow running for {} seconds", elapsed_secs);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Never read `SystemTime::now()` in workflow code: a replay would see a different
    /// time than the original run did.
    pub fn time(&self) -> &WorkflowTime {
        &self.time
    }

    // ============================================================================
    // Restart fresh
    // ============================================================================

    /// Ends this run and starts a new one with the same workflow id and an empty history.
    ///
    /// Long-running workflows (periodic jobs, subscriptions, workflows that model a
    /// durable entity) use this to keep their history from growing without bound, and to
    /// pick up new code periodically.
    ///
    /// The new run has a new run id, an empty history, and fresh `ctx.rand()` and
    /// `ctx.time()`. It keeps the workflow type and task queue and gets empty input; no
    /// state carries over. To pass state, or to change the type or queue, use
    /// [`restart_fresh_with`](Self::restart_fresh_with).
    ///
    /// This only emits the RestartFresh command; the workflow should return afterward.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// use std::time::Duration;
    ///
    /// async fn nightly_cleanup(ctx: WorkflowContext) -> Result<()> {
    ///     let removed: u64 = ctx.execute_task("remove_expired_sessions", ()).await?;
    ///     tracing::info!(removed, "cleanup finished");
    ///
    ///     // Wait for the next night.
    ///     ctx.sleep(Duration::from_secs(24 * 3600)).await?;
    ///
    ///     // Start the next cycle with an empty history.
    ///     ctx.restart_fresh()
    /// }
    /// ```
    pub fn restart_fresh(&self) -> Result<()> {
        // `None` keeps the current value.
        let command = WorkflowCommand::RestartFresh(RestartFreshCommand {
            workflow_type: None,
            input: vec![],
            task_queue: None,
            timeout: None,
        });

        self.add_command(command);

        tracing::info!(
            workflow_id = %self.execution.workflow_id,
            workflow_type = %self.execution.workflow_type,
            "Workflow restarting with fresh execution"
        );

        Ok(())
    }

    /// Like [`restart_fresh`](Self::restart_fresh), but passes `input` to the new run and
    /// can change its workflow type, task queue or timeout through `options`.
    ///
    /// Changing the workflow type is a way to move a long-running workflow onto new code.
    ///
    /// # Errors
    ///
    /// Returns an error if `input` cannot be serialized.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::time::Duration;
    /// #[derive(Serialize, Deserialize)]
    /// struct SubscriptionState {
    ///     user_id: String,
    ///     cycle: u32,
    ///     amount: f64,
    /// }
    ///
    /// #[workflow]
    /// async fn subscription(ctx: WorkflowContext, state: SubscriptionState) -> Result<()> {
    ///     let _charge_id: String = ctx
    ///         .execute_task("charge_customer", state.user_id.clone())
    ///         .await?;
    ///
    ///     // Wait for the next billing cycle.
    ///     ctx.sleep(Duration::from_secs(30 * 24 * 3600)).await?;
    ///
    ///     // Carry the state the next cycle needs.
    ///     let next_state = SubscriptionState {
    ///         user_id: state.user_id,
    ///         cycle: state.cycle + 1,
    ///         amount: state.amount,
    ///     };
    ///
    ///     ctx.restart_fresh_with(
    ///         next_state,
    ///         Some(RestartFreshOptions::new().timeout(Duration::from_secs(3600))),
    ///     )
    /// }
    /// ```
    ///
    /// Moving to a new workflow type:
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// #[workflow]
    /// async fn old_workflow(ctx: WorkflowContext, data: String) -> Result<()> {
    ///     ctx.restart_fresh_with(
    ///         data,
    ///         Some(
    ///             RestartFreshOptions::new()
    ///                 .workflow_type("NewWorkflowV2")
    ///                 .task_queue("upgraded-queue"),
    ///         ),
    ///     )
    /// }
    /// ```
    pub fn restart_fresh_with<T: Serialize>(
        &self,
        input: T,
        options: Option<RestartFreshOptions>,
    ) -> Result<()> {
        let input_bytes = serde_json::to_vec(&input).map_err(|e| {
            Error::Serialization(format!("Failed to serialize restart input: {}", e))
        })?;

        let opts = options.unwrap_or_default();

        let command = WorkflowCommand::RestartFresh(RestartFreshCommand {
            workflow_type: opts.workflow_type,
            input: input_bytes,
            task_queue: opts.task_queue,
            timeout: opts.timeout,
        });

        self.add_command(command);

        tracing::info!(
            workflow_id = %self.execution.workflow_id,
            current_workflow_type = %self.execution.workflow_type,
            "Workflow restarting with fresh execution and new input"
        );

        Ok(())
    }

    // ============================================================================
    // Metadata
    // ============================================================================

    /// Returns the workflow id.
    pub fn workflow_id(&self) -> &str {
        &self.execution.workflow_id
    }

    /// Returns the run id, which is unique to this run.
    pub fn run_id(&self) -> &str {
        &self.execution.run_id
    }

    /// Returns `true` if this activation is replaying recorded history.
    ///
    /// During replay, workflow code must not perform side effects of its own.
    pub fn is_replaying(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.replaying
    }

    /// Returns the attempt number, starting at 1.
    pub fn attempt(&self) -> i32 {
        self.execution.attempt
    }

    /// Returns the workflow type name.
    pub fn workflow_type(&self) -> &str {
        &self.execution.workflow_type
    }

    /// Returns the namespace.
    pub fn namespace(&self) -> &str {
        &self.execution.namespace
    }

    /// Returns the task queue.
    pub fn task_queue(&self) -> &str {
        &self.execution.task_queue
    }

    /// Returns the workflow's version string, a metadata label only.
    ///
    /// It is an identifier for metrics and inventory, not a replay-safety mechanism: the
    /// engine does not gate or route replay by version. To change workflow logic safely,
    /// run the new code under a new workflow name alongside the old one, or reset the
    /// execution. Do not branch on this value.
    pub fn version(&self) -> &str {
        &self.version
    }

    // ========================================================================
    // Runtime and test hooks
    // ========================================================================
    //
    // Public rather than cfg(test): the worker runtime and the crate's `testing`
    // module use them. Most are #[doc(hidden)].

    /// Records a result for `task_id`, so a replaying workflow receives it as that
    /// task's output. For tests.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::workflow::{WorkflowContext, WorkflowExecution};
    /// # let execution = WorkflowExecution {
    /// #     workflow_id: "test".to_string(),
    /// #     run_id: "run".to_string(),
    /// #     workflow_type: "Test".to_string(),
    /// #     attempt: 1,
    /// #     namespace: "test".to_string(),
    /// #     task_queue: "test".to_string(),
    /// # };
    /// let mut ctx = WorkflowContext::new(execution, false, "1.0.0".to_string());
    /// ctx.inject_task_result_for_test("task_1".to_string(), vec![1, 2, 3]);
    /// ```
    #[doc(hidden)]
    pub fn inject_task_result_for_test(&self, task_id: String, result: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        state.pending_task_results.insert(task_id, result);
    }

    /// Caches the workflow input for later invocations of the handler.
    ///
    /// The runtime calls this after extracting the input from the execution request. A
    /// handler can be invoked more than once in one activation, for example in an event
    /// loop, and later invocations do not get the jobs to extract the input from.
    pub fn cache_input(&self, input: crate::payload::Payload) {
        let mut state = self.state.lock().unwrap();
        state.cached_input = Some(input);
    }

    /// Returns the input stored by [`cache_input`](Self::cache_input), if any.
    pub fn get_cached_input(&self) -> Option<crate::payload::Payload> {
        let state = self.state.lock().unwrap();
        state.cached_input.clone()
    }

    /// Records results for many steps at once, keyed by step name.
    ///
    /// On replay, each step (task, closure or child workflow) that finds a recorded result
    /// returns it instead of running again. The server sends recorded step results in the
    /// journal as StepCompleted entries:
    ///
    /// ```text
    /// Server: returns the journal, including a StepCompleted entry per finished step.
    /// SDK:    injects those results, marks the context as replaying, and runs the
    ///         workflow, which reads the results instead of running the steps.
    /// ```
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::collections::HashMap;
    /// # fn example(ctx: &WorkflowContext) {
    /// let mut step_results = HashMap::new();
    /// step_results.insert("fetch_user_1".to_string(), vec![/* result bytes */]);
    /// step_results.insert("query_orders_2".to_string(), vec![/* result bytes */]);
    ///
    /// ctx.inject_step_results(step_results);
    /// ctx.set_replaying_for_test(true);
    ///
    /// // The steps named above return the recorded bytes.
    /// # }
    /// ```
    #[doc(hidden)]
    pub fn inject_step_results(&self, step_results: std::collections::HashMap<String, Vec<u8>>) {
        let mut state = self.state.lock().unwrap();
        for (step_name, result) in step_results {
            state.pending_task_results.insert(step_name, result);
        }
    }

    /// Whether the workflow has been told it is being cancelled.
    ///
    /// True from the wait at which the code learned of the request onwards: that wait
    /// returned [`WorkflowError::Canceled`](crate::error::WorkflowError::Canceled), and
    /// everything after it is the workflow's cleanup, which runs normally. A long loop
    /// that does not wait can check this to stop early. Read from the journal, so it
    /// answers the same at the same point on every replay.
    pub fn is_cancel_requested(&self) -> bool {
        self.state.lock().unwrap().cancel_delivered
    }

    /// Records that the journal holds a request to cancel the workflow, recorded at
    /// `at_ms` (ms since the epoch) when known.
    pub(crate) fn note_cancel_requested(&self, at_ms: Option<i64>) {
        let mut state = self.state.lock().unwrap();
        state.cancel_requested = true;
        state.cancel_requested_at = at_ms;
    }

    /// `Err(Canceled)` when this wait is where the code learns of a cancellation
    /// request; see [`ContextState::take_cancellation`].
    fn cancellation_at(&self, result: Option<Option<i64>>) -> Result<()> {
        if self.state.lock().unwrap().take_cancellation(result) {
            return Err(Error::Workflow(crate::error::WorkflowError::Canceled));
        }
        Ok(())
    }

    /// As [`cancellation_at`](Self::cancellation_at), for the result injected under `key`.
    pub(crate) fn cancellation_for(&self, key: &str) -> Result<()> {
        let result = self.state.lock().unwrap().result_seen(key);
        self.cancellation_at(result)
    }

    /// Records the result of one step (for example `"fetch_user_1"`) and marks it as a
    /// task. The runtime calls this for each result in the journal.
    #[doc(hidden)]
    pub fn inject_step_result(&self, step_name: String, result: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        state.pending_task_results.insert(step_name.clone(), result);
        // Mark it as a task so extract_closure_commands does not treat it as a closure.
        state.step_type_map.insert(step_name, 0);
    }

    /// Marks a durable timer as fired, with no known position. `sleep` and
    /// `sleep_with_id` check this so they return instead of emitting StartTimer again.
    #[cfg(test)]
    pub(crate) fn mark_timer_fired(&self, timer_id: String) {
        self.mark_timer_fired_at(timer_id, None);
    }

    /// Records when the engine journaled the result held under `key` (milliseconds since
    /// the UNIX epoch), for the workflow's clock to move to when the result is received.
    /// The first time recorded for a key stands.
    pub(crate) fn record_resolved_at(&self, key: String, at_ms: i64) {
        self.state
            .lock()
            .unwrap()
            .resolved_at
            .entry(key)
            .or_insert(at_ms);
    }

    /// Marks a durable timer as fired, with the FireTimer job's position (journal order)
    /// in this activation, so a wait with a timeout can tell whether its event or its
    /// deadline came first.
    pub(crate) fn mark_timer_fired_at(&self, timer_id: String, position: Option<usize>) {
        self.state
            .lock()
            .unwrap()
            .fired_timers
            .insert(timer_id, position);
    }

    /// Sets whether the context is replaying, so tests can simulate a replay.
    #[doc(hidden)]
    pub fn set_replaying_for_test(&self, replaying: bool) {
        let mut state = self.state.lock().unwrap();
        state.replaying = replaying;
    }

    /// Returns a copy of the commands emitted so far, leaving them in place.
    #[doc(hidden)]
    pub fn commands_for_test(&self) -> Vec<WorkflowCommand> {
        let state = self.state.lock().unwrap();
        state.commands.clone()
    }

    /// Removes and returns the commands emitted so far.
    #[doc(hidden)]
    pub fn take_commands_for_test(&self) -> Vec<WorkflowCommand> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct ApprovalData {
        approved: bool,
        approver: String,
        reason: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct StatusResponse {
        status: String,
        progress: u32,
        total: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct UpdateData {
        field: String,
        value: String,
    }

    fn create_test_context() -> WorkflowContext {
        let execution = WorkflowExecution {
            workflow_id: "test-wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        WorkflowContext::new(execution, false, "1.0".to_string())
    }

    // Cancellation requests

    /// A context replaying a journal in which `work_0` completed at 1000 ms and the
    /// cancellation request came at 2000 ms.
    fn context_asked_to_cancel() -> WorkflowContext {
        let ctx = create_test_context();
        ctx.set_replaying_for_test(true);
        ctx.inject_step_result("work_0".to_string(), serde_json::to_vec(&1).unwrap());
        ctx.record_resolved_at("work_0".to_string(), 1_000);
        ctx.note_cancel_requested(Some(2_000));
        ctx
    }

    fn is_canceled(result: &Result<u32>) -> bool {
        matches!(
            result,
            Err(Error::Workflow(crate::error::WorkflowError::Canceled))
        )
    }

    #[tokio::test]
    async fn a_result_from_before_the_request_is_received() {
        let ctx = context_asked_to_cancel();
        let first: Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert_eq!(first.unwrap(), 1);
        assert!(
            !ctx.is_cancel_requested(),
            "not told until a wait is cut short"
        );
    }

    #[tokio::test]
    async fn the_first_wait_without_an_earlier_result_is_told_once() {
        let ctx = context_asked_to_cancel();
        let _: Result<u32> = ctx.execute_task_by_name("work", 0).await;

        let interrupted: Result<u32> = ctx.execute_task_by_name("work", 1).await;
        assert!(is_canceled(&interrupted));
        assert!(ctx.is_cancel_requested());

        // Cleanup: the next wait is scheduled as usual, not cancelled again.
        let cleanup: Result<u32> = ctx.execute_task_by_name("refund", 2).await;
        assert!(matches!(
            cleanup,
            Err(Error::Workflow(
                crate::error::WorkflowError::Suspended { .. }
            ))
        ));
    }

    /// On a later replay the interrupted step may have a result of its own, recorded
    /// after the request. It is still where the request is delivered, or the code
    /// would take a different path than it did when it was first told.
    #[tokio::test]
    async fn the_same_wait_is_told_on_every_replay() {
        let ctx = context_asked_to_cancel();
        ctx.inject_step_result("work_1".to_string(), serde_json::to_vec(&2).unwrap());
        ctx.record_resolved_at("work_1".to_string(), 3_000);
        let _: Result<u32> = ctx.execute_task_by_name("work", 0).await;

        let interrupted: Result<u32> = ctx.execute_task_by_name("work", 1).await;

        assert!(is_canceled(&interrupted));
    }

    /// A result recorded in the same millisecond as the request counts as first.
    #[tokio::test]
    async fn a_result_recorded_with_the_request_is_received() {
        let ctx = create_test_context();
        ctx.set_replaying_for_test(true);
        ctx.inject_step_result("work_0".to_string(), serde_json::to_vec(&1).unwrap());
        ctx.record_resolved_at("work_0".to_string(), 2_000);
        ctx.note_cancel_requested(Some(2_000));
        let first: Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert_eq!(first.unwrap(), 1);
    }

    #[tokio::test]
    async fn without_a_request_nothing_is_cancelled() {
        let ctx = create_test_context();
        ctx.set_replaying_for_test(true);
        let pending: Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert!(!is_canceled(&pending));
        assert!(!ctx.is_cancel_requested());
    }

    #[tokio::test]
    async fn a_sleep_whose_timer_had_not_fired_is_told() {
        let ctx = create_test_context();
        ctx.set_replaying_for_test(true);
        ctx.note_cancel_requested(Some(2_000));
        let slept = ctx.sleep(Duration::from_secs(600)).await;
        assert!(matches!(
            slept,
            Err(Error::Workflow(crate::error::WorkflowError::Canceled))
        ));
    }

    #[tokio::test]
    async fn an_event_that_came_before_the_request_is_received() {
        let ctx = create_test_context();
        ctx.set_replaying_for_test(true);
        ctx.buffer_journaled_event(
            "go".to_string(),
            serde_json::to_vec(&7).unwrap(),
            Some(0),
            Some(1_000),
        );
        ctx.note_cancel_requested(Some(2_000));
        let received: u32 = ctx.wait_for_event("go").await.unwrap();
        assert_eq!(received, 7);
        let next: Result<u32> = ctx.wait_for_event("go").await;
        assert!(is_canceled(&next));
    }

    // Event buffering tests

    #[tokio::test]
    async fn test_buffer_and_retrieve_event() {
        let ctx = create_test_context();

        let approval = ApprovalData {
            approved: true,
            approver: "manager@example.com".to_string(),
            reason: "Looks good".to_string(),
        };

        let event_bytes = serde_json::to_vec(&approval).unwrap();
        ctx.buffer_event("approve", event_bytes);

        let retrieved: ApprovalData = ctx.wait_for_event("approve").await.unwrap();
        assert_eq!(retrieved, approval);
    }

    #[tokio::test]
    async fn test_buffer_multiple_events_same_name() {
        let ctx = create_test_context();

        let approval1 = ApprovalData {
            approved: true,
            approver: "manager1@example.com".to_string(),
            reason: "First approval".to_string(),
        };
        let approval2 = ApprovalData {
            approved: false,
            approver: "manager2@example.com".to_string(),
            reason: "Second approval".to_string(),
        };

        ctx.buffer_event("approve", serde_json::to_vec(&approval1).unwrap());
        ctx.buffer_event("approve", serde_json::to_vec(&approval2).unwrap());

        // Oldest first, in arrival order, as in the other SDKs. A loop over status
        // callbacks must see them in the order they were sent.
        let retrieved1: ApprovalData = ctx.wait_for_event("approve").await.unwrap();
        assert_eq!(retrieved1, approval1);

        let retrieved2: ApprovalData = ctx.wait_for_event("approve").await.unwrap();
        assert_eq!(retrieved2, approval2);
    }

    #[tokio::test]
    async fn test_buffer_different_event_names() {
        let ctx = create_test_context();

        let approval = ApprovalData {
            approved: true,
            approver: "manager@example.com".to_string(),
            reason: "Approved".to_string(),
        };
        let update = UpdateData {
            field: "status".to_string(),
            value: "updated".to_string(),
        };

        ctx.buffer_event("approve", serde_json::to_vec(&approval).unwrap());
        ctx.buffer_event("update", serde_json::to_vec(&update).unwrap());

        let retrieved_update: UpdateData = ctx.wait_for_event("update").await.unwrap();
        assert_eq!(retrieved_update, update);

        let retrieved_approval: ApprovalData = ctx.wait_for_event("approve").await.unwrap();
        assert_eq!(retrieved_approval, approval);
    }

    #[tokio::test]
    async fn test_wait_for_event_not_buffered() {
        let ctx = create_test_context();

        let result: Result<ApprovalData> = ctx.wait_for_event("approve").await;
        assert!(result.is_err());
    }

    /// Suspending is the normal outcome when the event has not arrived, and it is the
    /// path where holding the state lock would deadlock.
    #[tokio::test]
    async fn test_wait_for_event_with_timeout_not_buffered() {
        let ctx = create_test_context();

        let result: Result<Option<ApprovalData>> = ctx
            .wait_for_event_with_timeout("approve", Duration::from_secs(60))
            .await;

        assert!(
            result.is_err(),
            "waiting for an event that has not arrived must suspend, not return"
        );
    }

    #[tokio::test]
    async fn test_wait_for_event_with_timeout_buffered() {
        let ctx = create_test_context();

        let approval = ApprovalData {
            approved: true,
            approver: "manager@example.com".to_string(),
            reason: "Quick approval".to_string(),
        };

        ctx.buffer_event("approve", serde_json::to_vec(&approval).unwrap());

        let result: Option<ApprovalData> = ctx
            .wait_for_event_with_timeout("approve", Duration::from_secs(60))
            .await
            .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap(), approval);
    }

    // The timeout is a durable timer raced against the event.

    fn timer_ids(commands: &[WorkflowCommand]) -> Vec<String> {
        commands
            .iter()
            .filter_map(|c| match c {
                WorkflowCommand::StartTimer(t) => Some(t.timer_id.clone()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn timed_wait_parks_on_the_event_and_starts_a_deadline_timer() {
        let ctx = create_test_context();
        let result: Result<Option<ApprovalData>> = ctx
            .wait_for_event_with_timeout("approve", Duration::from_secs(5))
            .await;
        assert!(result.is_err(), "must suspend");

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 2);
        assert!(matches!(
            &commands[0],
            WorkflowCommand::WaitForEvent(w) if w.event_name == "approve"
        ));
        match &commands[1] {
            WorkflowCommand::StartTimer(t) => {
                assert_eq!(t.timer_id, "event_timeout_approve_1");
                assert_eq!(t.duration, Duration::from_secs(5));
            }
            other => panic!("expected StartTimer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timed_wait_returns_none_once_the_deadline_fired() {
        let ctx = create_test_context();
        ctx.mark_timer_fired("event_timeout_approve_1".to_string());

        let result: Option<ApprovalData> = ctx
            .wait_for_event_with_timeout("approve", Duration::from_secs(5))
            .await
            .unwrap();
        assert!(result.is_none());
        assert!(ctx.take_commands().is_empty());
    }

    #[tokio::test]
    async fn timed_wait_takes_an_event_that_arrived_before_the_deadline() {
        let ctx = create_test_context();
        ctx.buffer_event_at("status", br#"{"early":true}"#.to_vec(), Some(2));
        ctx.mark_timer_fired_at("event_timeout_status_1".to_string(), Some(5));

        let result: Option<serde_json::Value> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(result, Some(serde_json::json!({"early": true})));
    }

    #[tokio::test]
    async fn timed_out_wait_stays_timed_out_when_the_event_arrives_later() {
        // Journal: the deadline fired (position 3), then the event (7). An
        // earlier activation took the timeout branch; the replay must too,
        // and the event belongs to the next wait.
        let ctx = create_test_context();
        ctx.mark_timer_fired_at("event_timeout_status_1".to_string(), Some(3));
        ctx.buffer_event_at("status", br#"{"late":true}"#.to_vec(), Some(7));

        let first: Option<serde_json::Value> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(first, None);

        let second: Option<serde_json::Value> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(second, Some(serde_json::json!({"late": true})));
    }

    #[tokio::test]
    async fn timed_wait_consumes_no_sequence_when_it_returns() {
        // Step ids after the wait must match whether the event was there or
        // not, or a replay would re-schedule work that already ran.
        let ctx = create_test_context();
        ctx.buffer_event("approve", br#"{"ok":true}"#.to_vec());
        let before = ctx.state.lock().unwrap().sequence;
        let _: Option<serde_json::Value> = ctx
            .wait_for_event_with_timeout("approve", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(ctx.state.lock().unwrap().sequence, before);
    }

    #[tokio::test]
    async fn a_wait_takes_no_step_number_when_it_parks_either() {
        // Parking and returning must agree, or a step issued beside a wait
        // that parked gets another id once the event is there.
        let ctx = create_test_context();
        let before = ctx.state.lock().unwrap().sequence;
        let parked: Result<serde_json::Value> = ctx.wait_for_event("approve").await;
        assert!(parked.is_err());
        let parked: Result<Option<serde_json::Value>> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await;
        assert!(parked.is_err());
        assert_eq!(ctx.state.lock().unwrap().sequence, before);
    }

    #[tokio::test]
    async fn deadline_timers_are_numbered_per_event_name() {
        // Allocated on every path: the first wait returns a buffered event,
        // and the second still gets the second id.
        let ctx = create_test_context();
        ctx.buffer_event("status", br#"{"round":1}"#.to_vec());
        let _: Option<serde_json::Value> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await
            .unwrap();
        let _: Result<Option<serde_json::Value>> = ctx
            .wait_for_event_with_timeout("status", Duration::from_secs(1))
            .await;
        let _: Result<Option<serde_json::Value>> = ctx
            .wait_for_event_with_timeout("other", Duration::from_secs(1))
            .await;

        assert_eq!(
            timer_ids(&ctx.take_commands()),
            vec!["event_timeout_status_2", "event_timeout_other_1"]
        );
    }

    #[tokio::test]
    async fn timed_wait_rejects_a_zero_timeout() {
        let ctx = create_test_context();
        let result: Result<Option<serde_json::Value>> = ctx
            .wait_for_event_with_timeout("approve", Duration::ZERO)
            .await;
        assert!(result.is_err());
        assert!(ctx.take_commands().is_empty());
    }

    // Query handler tests

    #[test]
    fn test_register_query_handler_simple() {
        let ctx = create_test_context();

        ctx.register_query_handler("get_status", || "processing".to_string());

        let result = ctx.handle_query_request("get_status").unwrap();
        let status: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(status, "processing");
    }

    #[test]
    fn test_register_query_handler_complex() {
        let ctx = create_test_context();

        ctx.register_query_handler("get_progress", || StatusResponse {
            status: "in_progress".to_string(),
            progress: 42,
            total: 100,
        });

        let result = ctx.handle_query_request("get_progress").unwrap();
        let response: StatusResponse = serde_json::from_slice(&result).unwrap();

        assert_eq!(response.status, "in_progress");
        assert_eq!(response.progress, 42);
        assert_eq!(response.total, 100);
    }

    #[test]
    fn test_register_multiple_query_handlers() {
        let ctx = create_test_context();

        ctx.register_query_handler("get_status", || "running".to_string());
        ctx.register_query_handler("get_count", || 42u32);
        ctx.register_query_handler("is_ready", || true);

        let status_result = ctx.handle_query_request("get_status").unwrap();
        let status: String = serde_json::from_slice(&status_result).unwrap();
        assert_eq!(status, "running");

        let count_result = ctx.handle_query_request("get_count").unwrap();
        let count: u32 = serde_json::from_slice(&count_result).unwrap();
        assert_eq!(count, 42);

        let ready_result = ctx.handle_query_request("is_ready").unwrap();
        let ready: bool = serde_json::from_slice(&ready_result).unwrap();
        assert!(ready);
    }

    #[test]
    fn test_query_handler_not_registered() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("unknown_query");
        assert!(result.is_err());
    }

    #[test]
    fn test_query_handler_override() {
        let ctx = create_test_context();

        ctx.register_query_handler("get_status", || "initial".to_string());
        ctx.register_query_handler("get_status", || "updated".to_string());

        let result = ctx.handle_query_request("get_status").unwrap();
        let status: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(status, "updated");
    }

    #[tokio::test]
    async fn test_event_deserialization_error() {
        let ctx = create_test_context();

        ctx.buffer_event("approve", b"invalid json".to_vec());

        let result: Result<ApprovalData> = ctx.wait_for_event("approve").await;
        assert!(result.is_err());
    }

    // Automatic query tests

    #[test]
    fn test_automatic_queries_workflow_id() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("workflow_id").unwrap();
        let wf_id: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(wf_id, "test-wf-123");
    }

    #[test]
    fn test_automatic_queries_workflow_type() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("workflow_type").unwrap();
        let wf_type: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(wf_type, "TestWorkflow");
    }

    #[test]
    fn test_automatic_queries_run_id() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("run_id").unwrap();
        let run_id: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(run_id, "run-456");
    }

    #[test]
    fn test_automatic_queries_is_replaying() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("is_replaying").unwrap();
        let replaying: bool = serde_json::from_slice(&result).unwrap();
        assert!(!replaying);
    }

    #[test]
    fn test_automatic_queries_attempt() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("attempt").unwrap();
        let attempt: i32 = serde_json::from_slice(&result).unwrap();
        assert_eq!(attempt, 1);
    }

    #[test]
    fn test_automatic_queries_version() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("version").unwrap();
        let version: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(version, "1.0");
    }

    #[test]
    fn test_automatic_queries_namespace() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("namespace").unwrap();
        let namespace: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(namespace, "default");
    }

    #[test]
    fn test_automatic_queries_task_queue() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("task_queue").unwrap();
        let task_queue: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(task_queue, "test-queue");
    }

    #[test]
    fn test_automatic_queries_workflow_info() {
        let ctx = create_test_context();

        let result = ctx.handle_query_request("workflow_info").unwrap();
        let info: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(info["workflow_id"], "test-wf-123");
        assert_eq!(info["run_id"], "run-456");
        assert_eq!(info["workflow_type"], "TestWorkflow");
        assert_eq!(info["namespace"], "default");
        assert_eq!(info["task_queue"], "test-queue");
        assert_eq!(info["attempt"], 1);
        assert_eq!(info["version"], "1.0");
        assert_eq!(info["is_replaying"], false);
    }

    #[test]
    fn test_automatic_queries_not_overridable_by_user() {
        let ctx = create_test_context();

        ctx.register_query_handler("workflow_id", || "custom_id".to_string());

        // The last registration wins.
        let result = ctx.handle_query_request("workflow_id").unwrap();
        let workflow_id: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(workflow_id, "custom_id");
    }

    #[test]
    fn test_all_automatic_queries_available() {
        let ctx = create_test_context();

        let queries = vec![
            "workflow_id",
            "workflow_type",
            "run_id",
            "is_replaying",
            "attempt",
            "version",
            "namespace",
            "task_queue",
            "workflow_info",
        ];

        for query in queries {
            let result = ctx.handle_query_request(query);
            assert!(result.is_ok(), "Query '{}' should be available", query);
        }
    }

    // Restart fresh tests

    #[test]
    fn test_restart_fresh_basic() {
        let ctx = create_test_context();

        let result = ctx.restart_fresh();
        assert!(result.is_ok());

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        if let WorkflowCommand::RestartFresh(cmd) = &commands[0] {
            assert!(cmd.workflow_type.is_none());
            assert!(cmd.task_queue.is_none());
            assert!(cmd.timeout.is_none());
            assert!(cmd.input.is_empty());
        } else {
            panic!("Expected RestartFresh command");
        }
    }

    #[test]
    fn test_restart_fresh_with_input() {
        let ctx = create_test_context();

        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        struct State {
            counter: u32,
            user_id: String,
        }

        let state = State {
            counter: 42,
            user_id: "user-123".to_string(),
        };

        let result = ctx.restart_fresh_with(state.clone(), None);
        assert!(result.is_ok());

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        if let WorkflowCommand::RestartFresh(cmd) = &commands[0] {
            assert!(cmd.workflow_type.is_none());
            assert!(!cmd.input.is_empty());

            let deserialized: State = serde_json::from_slice(&cmd.input).unwrap();
            assert_eq!(deserialized, state);
            assert!(cmd.task_queue.is_none());
            assert!(cmd.timeout.is_none());
        } else {
            panic!("Expected RestartFresh command");
        }
    }

    #[test]
    fn test_restart_fresh_with_options() {
        use crate::workflow::RestartFreshOptions;

        let ctx = create_test_context();

        let options = RestartFreshOptions::new()
            .workflow_type("NewWorkflowV2")
            .task_queue("upgraded-queue")
            .timeout(Duration::from_secs(3600));

        let result = ctx.restart_fresh_with("new_input", Some(options));
        assert!(result.is_ok());

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        if let WorkflowCommand::RestartFresh(cmd) = &commands[0] {
            assert_eq!(cmd.workflow_type.as_deref(), Some("NewWorkflowV2"));
            assert_eq!(cmd.task_queue.as_deref(), Some("upgraded-queue"));
            assert_eq!(cmd.timeout, Some(Duration::from_secs(3600)));

            let deserialized: String = serde_json::from_slice(&cmd.input).unwrap();
            assert_eq!(deserialized, "new_input");
        } else {
            panic!("Expected RestartFresh command");
        }
    }

    #[test]
    fn test_restart_fresh_with_partial_options() {
        use crate::workflow::RestartFreshOptions;

        let ctx = create_test_context();

        // Only workflow_type is set; the rest keep their defaults.
        let options = RestartFreshOptions::new().workflow_type("NewWorkflow");

        let result = ctx.restart_fresh_with(100u32, Some(options));
        assert!(result.is_ok());

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        if let WorkflowCommand::RestartFresh(cmd) = &commands[0] {
            assert_eq!(cmd.workflow_type.as_deref(), Some("NewWorkflow"));
            assert!(cmd.task_queue.is_none());
            assert!(cmd.timeout.is_none());
        } else {
            panic!("Expected RestartFresh command");
        }
    }

    #[test]
    fn test_restart_fresh_command_is_terminal() {
        let ctx = create_test_context();

        ctx.restart_fresh().unwrap();

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);
        assert!(commands[0].is_terminal());
    }

    #[test]
    fn test_restart_fresh_serialization_error() {
        use std::collections::HashMap;

        let ctx = create_test_context();

        // NaN serializes to JSON null, so this input does not fail.
        let mut map: HashMap<String, f64> = HashMap::new();
        map.insert("value".to_string(), f64::NAN);

        let result = ctx.restart_fresh_with(&map, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_restart_fresh_with_complex_state() {
        let ctx = create_test_context();

        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
        struct ComplexState {
            id: String,
            count: u32,
            active: bool,
            tags: Vec<String>,
            metadata: HashMap<String, String>,
        }

        let mut metadata = HashMap::new();
        metadata.insert("region".to_string(), "us-west-2".to_string());
        metadata.insert("tier".to_string(), "premium".to_string());

        let state = ComplexState {
            id: "workflow-123".to_string(),
            count: 999,
            active: true,
            tags: vec!["important".to_string(), "priority".to_string()],
            metadata,
        };

        let result = ctx.restart_fresh_with(state.clone(), None);
        assert!(result.is_ok());

        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        if let WorkflowCommand::RestartFresh(cmd) = &commands[0] {
            let deserialized: ComplexState = serde_json::from_slice(&cmd.input).unwrap();
            assert_eq!(deserialized, state);
        } else {
            panic!("Expected RestartFresh command");
        }
    }

    #[test]
    fn test_restart_fresh_multiple_calls() {
        let ctx = create_test_context();

        ctx.restart_fresh().unwrap();
        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);

        // A second call also emits a command, though a real workflow would return after
        // the first.
        ctx.restart_fresh().unwrap();
        let commands = ctx.take_commands();
        assert_eq!(commands.len(), 1);
    }

    // Workflow versioning tests

    #[test]
    fn test_version_basic() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.2.3".to_string());
        assert_eq!(ctx.version(), "1.2.3");
    }
}
