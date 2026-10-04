//! Execution runtime: runs workflow and task handlers on work polled by sdk-core.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};

use crate::error::{Error, Result};
use crate::payload::Payload;
use crate::worker::registration::{TaskHandlerFn, WorkflowHandlerFn};
use crate::workflow::closure_commands::extract_closure_commands;
use crate::workflow::{WorkflowCommand, WorkflowContext, WorkflowExecution};
use crate::TaskContext;

use orcher_sdk_core::bridge::{
    Command as BridgeCommand, CompleteWorkflowCommand, ExecutionResult, FailWorkflowCommand,
    QueryResponse, QueryResult, RequestJob, ScheduleTaskCommand, StartTimerCommand,
    TaskRetryPolicy as BridgeRetryPolicy, UpdateResponse, UpdateResult, WaitForEventCommand,
};
use orcher_sdk_core::poller::{TaskExecutionTask, WorkflowExecutionTask};
use orcher_sdk_core::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};

/// A workflow task handed to the runtime, with the channel its result goes back on.
#[derive(Debug)]
pub struct WorkflowExecutionRequest {
    /// The workflow task polled from the server.
    pub task: WorkflowExecutionTask,
    /// Receives the outcome once the workflow handler has run.
    pub result_sender: tokio::sync::oneshot::Sender<WorkflowExecutionResponse>,
}

/// The outcome of a [`WorkflowExecutionRequest`].
#[derive(Debug)]
pub struct WorkflowExecutionResponse {
    /// The commands the workflow produced, or the error that stopped it.
    pub result: orcher_sdk_core::Result<ExecutionResult>,
}

/// A task handed to the runtime, with the channel its result goes back on.
#[derive(Debug)]
pub struct TaskExecutionRequest {
    /// The task polled from the server.
    pub task: TaskExecutionTask,
    /// Receives the outcome once the task handler has run.
    pub result_sender: tokio::sync::oneshot::Sender<TaskExecutionResponse>,
}

/// The outcome of a [`TaskExecutionRequest`].
#[derive(Debug)]
pub struct TaskExecutionResponse {
    /// The task's encoded output, or the error it failed with.
    pub result: orcher_sdk_core::Result<Vec<u8>>,
}

/// Limits and defaults for an [`ExecutionRuntime`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RuntimeConfig {
    /// Workflow handlers allowed to run at the same time.
    pub max_concurrent_workflows: usize,
    /// Task handlers allowed to run at the same time.
    pub max_concurrent_tasks: usize,
    /// Queue that scheduled tasks go to when a workflow names none.
    pub default_task_queue: String,
    /// Namespace this runtime executes in.
    pub namespace: String,
    /// Codec chain that decodes compressed or encrypted inputs.
    ///
    /// If decoding fails, a warning is logged and the handler gets the input
    /// as received.
    pub codec_chain: Option<Arc<orcher_sdk_core::codec::CodecChain>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_workflows: 100,
            max_concurrent_tasks: 200,
            default_task_queue: "default".to_string(),
            namespace: "default".to_string(),
            codec_chain: None,
        }
    }
}

/// Snapshot of execution statistics.
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    /// Workflow tasks started.
    pub workflows_executed: u64,
    /// Workflow tasks that returned a result.
    pub workflows_succeeded: u64,
    /// Workflow tasks that failed.
    pub workflows_failed: u64,
    /// Tasks started.
    pub tasks_executed: u64,
    /// Tasks that returned a result.
    pub tasks_succeeded: u64,
    /// Tasks that failed.
    pub tasks_failed: u64,
}

/// Runs registered workflow and task handlers on work received from sdk-core.
///
/// Semaphores cap how many workflow and task handlers run at once.
pub struct ExecutionRuntime {
    config: RuntimeConfig,
    workflow_handlers: Arc<RwLock<HashMap<String, WorkflowHandlerFn>>>,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandlerFn>>>,
    workflow_semaphore: Arc<Semaphore>,
    task_semaphore: Arc<Semaphore>,
    /// Workflow inputs by run ID, kept across activations of a run because a
    /// later activation may not carry the start job. Cleared when the run
    /// completes.
    input_cache: Arc<RwLock<HashMap<String, Payload>>>,

    // Counters reported by `stats()`.
    workflows_executed: AtomicU64,
    workflows_succeeded: AtomicU64,
    workflows_failed: AtomicU64,
    tasks_executed: AtomicU64,
    tasks_succeeded: AtomicU64,
    tasks_failed: AtomicU64,
    last_success_time: std::sync::Mutex<Option<Instant>>,
}

impl ExecutionRuntime {
    /// Creates a runtime with no handlers registered.
    pub fn new(config: RuntimeConfig) -> Self {
        let workflow_semaphore = Arc::new(Semaphore::new(config.max_concurrent_workflows));
        let task_semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

        Self {
            config,
            workflow_handlers: Arc::new(RwLock::new(HashMap::new())),
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            workflow_semaphore,
            task_semaphore,
            input_cache: Arc::new(RwLock::new(HashMap::new())),
            workflows_executed: AtomicU64::new(0),
            workflows_succeeded: AtomicU64::new(0),
            workflows_failed: AtomicU64::new(0),
            tasks_executed: AtomicU64::new(0),
            tasks_succeeded: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            last_success_time: std::sync::Mutex::new(None),
        }
    }

    /// Current execution counters.
    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            workflows_executed: self.workflows_executed.load(Ordering::Relaxed),
            workflows_succeeded: self.workflows_succeeded.load(Ordering::Relaxed),
            workflows_failed: self.workflows_failed.load(Ordering::Relaxed),
            tasks_executed: self.tasks_executed.load(Ordering::Relaxed),
            tasks_succeeded: self.tasks_succeeded.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
        }
    }

    /// When a workflow or task last succeeded, if one has.
    pub fn last_success_time(&self) -> Option<Instant> {
        *self.last_success_time.lock().unwrap()
    }

    /// Registers the handler for workflows of `workflow_type`, replacing any
    /// earlier one.
    pub async fn register_workflow_handler(
        &self,
        workflow_type: impl Into<String>,
        handler: WorkflowHandlerFn,
    ) {
        let workflow_type = workflow_type.into();
        tracing::info!(workflow_type = %workflow_type, "Registering workflow handler in runtime");
        self.workflow_handlers
            .write()
            .await
            .insert(workflow_type, handler);
    }

    /// Registers the handler for tasks of `task_type`, replacing any earlier one.
    pub async fn register_task_handler(
        &self,
        task_type: impl Into<String>,
        handler: TaskHandlerFn,
    ) {
        let task_type = task_type.into();
        tracing::info!(task_type = %task_type, "Registering task handler in runtime");
        self.task_handlers.write().await.insert(task_type, handler);
    }

    /// Runs one workflow activation through its registered handler and returns
    /// the commands it produced.
    ///
    /// Failures, such as an unregistered workflow type, an unreadable input or a
    /// handler error, are reported inside the [`ExecutionResult`].
    pub async fn execute_workflow(&self, task: WorkflowExecutionTask) -> ExecutionResult {
        let workflow_id = task.execution.workflow_id.clone();
        let run_id = task.execution.run_id.clone();
        let workflow_type = task.workflow_type.clone();

        self.workflows_executed.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            workflow_id = %workflow_id,
            run_id = %run_id,
            workflow_type = %workflow_type,
            "Executing workflow in sdk-rust runtime"
        );

        let _permit = match self.workflow_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(e) => {
                tracing::error!(error = %e, "Failed to acquire workflow semaphore");
                return ExecutionResult::failed(
                    run_id,
                    orcher_sdk_core::bridge::ExecutionError {
                        message: format!("Failed to acquire execution permit: {}", e),
                        error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                        details: None,
                        retryable: true,
                    },
                );
            }
        };

        let handler = {
            let handlers = self.workflow_handlers.read().await;
            handlers.get(&workflow_type).cloned()
        };

        let handler = match handler {
            Some(h) => h,
            None => {
                tracing::error!(
                    workflow_type = %workflow_type,
                    "No handler registered for workflow type"
                );
                return ExecutionResult::failed(
                    run_id,
                    orcher_sdk_core::bridge::ExecutionError {
                        message: format!(
                            "No handler registered for workflow type: {}",
                            workflow_type
                        ),
                        error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                        details: None,
                        retryable: false,
                    },
                );
            }
        };

        self.execute_workflow_handler(task, handler).await
    }

    async fn execute_workflow_handler(
        &self,
        task: WorkflowExecutionTask,
        handler: WorkflowHandlerFn,
    ) -> ExecutionResult {
        let workflow_id = task.execution.workflow_id.clone();
        let run_id = task.execution.run_id.clone();
        let workflow_type = task.workflow_type.clone();
        let task_queue = task.task_queue.clone();

        let execution_request =
            match orcher_sdk_core::bridge::task_to_execution_request(task.clone()) {
                Ok(req) => req,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert task to execution request");
                    return ExecutionResult::failed(
                        run_id,
                        orcher_sdk_core::bridge::ExecutionError {
                            message: format!("Failed to convert task: {}", e),
                            error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                            details: None,
                            retryable: false,
                        },
                    );
                }
            };

        let execution = WorkflowExecution {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
            workflow_type: workflow_type.clone(),
            attempt: task.attempt,
            namespace: self.config.namespace.clone(),
            task_queue: task_queue.clone(),
        };

        // The workflow's clock comes from the journal, never from this
        // machine: it starts when the engine journaled the start, and moves to
        // when each result the workflow receives was journaled.
        let journal_times = JournalTimes::read(&task.journal);
        let started_at_ms = journal_times.started_at_ms.unwrap_or_else(|| {
            tracing::warn!(
                workflow_id = %workflow_id,
                "The journal records no start time; workflow time starts now"
            );
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });
        let ctx = WorkflowContext::starting_at(
            execution,
            execution_request.is_replaying,
            "1.0.0".to_string(),
            started_at_ms,
        );

        // On replay, give the context the recorded results so completed steps
        // return them instead of running again.
        if execution_request.is_replaying {
            self.inject_step_results(&ctx, &execution_request.jobs, journal_times)
                .await;
        }

        let input = match self
            .get_or_extract_input(&run_id, &execution_request.jobs)
            .await
        {
            Ok(input) => input,
            Err(e) => {
                tracing::error!(error = %e, "Failed to get workflow input");
                return ExecutionResult::failed(
                    run_id,
                    orcher_sdk_core::bridge::ExecutionError {
                        message: format!("Failed to get workflow input: {}", e),
                        error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                        details: None,
                        retryable: false,
                    },
                );
            }
        };

        let handler_result = handler(ctx.clone(), input).await;

        // Queries and updates run after the handler, because the handler is
        // what registers their handlers on the context.
        let (query_responses, update_results) = self
            .process_query_and_update_jobs(&ctx, &execution_request.jobs)
            .await;
        // The steps the code reached, which sdk-core checks against the
        // journal to tell code that no longer replays the run.
        let reached_steps = ctx.take_reached_steps();

        let mut result = match handler_result {
            Ok(output) => {
                self.cleanup_input_cache(&run_id).await;
                let r = self.handle_workflow_completion(
                    output,
                    ctx,
                    run_id,
                    &task_queue,
                    query_responses,
                    update_results,
                );
                self.workflows_succeeded.fetch_add(1, Ordering::Relaxed);
                *self.last_success_time.lock().unwrap() = Some(Instant::now());
                r
            }
            Err(Error::Workflow(crate::error::WorkflowError::Suspended {
                reason,
                pending_operations,
            })) => {
                tracing::info!(
                    workflow_id = %workflow_id,
                    reason = %reason,
                    pending_count = pending_operations.len(),
                    "Workflow suspended"
                );
                // Suspension is not a failure, so it is not counted as one.
                self.handle_workflow_suspension(
                    ctx,
                    run_id,
                    &task_queue,
                    query_responses,
                    update_results,
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "Workflow handler failed");
                self.workflows_failed.fetch_add(1, Ordering::Relaxed);
                ExecutionResult::failed(run_id, workflow_failure(&e))
            }
        };
        result.set_reached_steps(reached_steps);
        result
    }

    fn handle_workflow_completion(
        &self,
        output: Payload,
        ctx: WorkflowContext,
        run_id: String,
        task_queue: &str,
        query_responses: Vec<QueryResponse>,
        update_results: Vec<UpdateResponse>,
    ) -> ExecutionResult {
        let sdk_commands = ctx.take_commands();

        let mut bridge_commands = self.closure_result_commands(&ctx, task_queue);

        for cmd in sdk_commands {
            match self.convert_command(cmd, task_queue) {
                Ok(bridge_cmd) => bridge_commands.push(bridge_cmd),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert command");
                    return ExecutionResult::failed(
                        run_id,
                        orcher_sdk_core::bridge::ExecutionError {
                            message: format!("Command conversion failed: {}", e),
                            error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                            details: None,
                            retryable: false,
                        },
                    );
                }
            }
        }

        // Complete the workflow only if it did not already emit a terminal
        // command. A workflow that calls `restart_fresh` emits RestartFresh and
        // then returns normally; adding CompleteWorkflow as well would give two
        // terminal commands, which the bridge rejects ("Cannot have multiple
        // terminal commands"). When the workflow restarts, its return value is
        // deliberately discarded.
        let already_terminal = bridge_commands.iter().any(|c| {
            matches!(
                c,
                BridgeCommand::CompleteWorkflow(_)
                    | BridgeCommand::FailWorkflow(_)
                    | BridgeCommand::RestartFresh(_)
                    | BridgeCommand::CancelWorkflowExecution(_)
            )
        });
        if !already_terminal {
            bridge_commands.push(BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                result: Payload::new_data(output.data),
            }));
        }

        let mut result = ExecutionResult::success(run_id, bridge_commands);
        for qr in query_responses {
            result.add_query_response(qr);
        }
        for ur in update_results {
            result.add_update_response(ur);
        }
        result
    }

    /// `RecordStepResult` commands for the closures that ran in this activation, in the
    /// order they ran.
    ///
    /// They go ahead of the activation's other commands: those include any terminal
    /// command, which must come last, and the step the workflow suspended on, which it
    /// issued after the closures had finished. A closure command that cannot be
    /// converted is logged and skipped.
    fn closure_result_commands(
        &self,
        ctx: &WorkflowContext,
        task_queue: &str,
    ) -> Vec<BridgeCommand> {
        let mut closure_commands = match extract_closure_commands(ctx) {
            Ok(cmds) => cmds,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to extract closure commands");
                return Vec::new();
            }
        };
        // Extracted from a map; a closure's step id ends in its step number.
        closure_commands.sort_by_key(|cmd| {
            cmd.step_name
                .rsplit('_')
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(u64::MAX)
        });
        closure_commands
            .into_iter()
            .filter_map(|cmd| {
                self.convert_command(WorkflowCommand::RecordStepResult(cmd), task_queue)
                    .map_err(|e| tracing::warn!(error = %e, "Failed to convert closure command"))
                    .ok()
            })
            .collect()
    }

    fn handle_workflow_suspension(
        &self,
        ctx: WorkflowContext,
        run_id: String,
        task_queue: &str,
        query_responses: Vec<QueryResponse>,
        update_results: Vec<UpdateResponse>,
    ) -> ExecutionResult {
        let sdk_commands = ctx.take_commands();

        if sdk_commands.is_empty() {
            return ExecutionResult::failed(
                run_id,
                orcher_sdk_core::bridge::ExecutionError {
                    message: "Workflow suspended without generating commands".to_string(),
                    error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                    details: None,
                    retryable: false,
                },
            );
        }

        // The closures that ran in this activation are reported with it. A result
        // reported only on completion is not in the journal the next activation
        // replays, so the closure would run again on every activation until the
        // workflow finished.
        let mut bridge_commands = self.closure_result_commands(&ctx, task_queue);

        for cmd in sdk_commands {
            match self.convert_command(cmd, task_queue) {
                Ok(bridge_cmd) => bridge_commands.push(bridge_cmd),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to convert command during suspension");
                    return ExecutionResult::failed(
                        run_id,
                        orcher_sdk_core::bridge::ExecutionError {
                            message: format!("Command conversion failed: {}", e),
                            error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                            details: None,
                            retryable: false,
                        },
                    );
                }
            }
        }

        let mut result = ExecutionResult::success(run_id, bridge_commands);
        for qr in query_responses {
            result.add_query_response(qr);
        }
        for ur in update_results {
            result.add_update_response(ur);
        }
        result
    }

    /// Runs one task through its registered handler and returns its encoded output.
    ///
    /// `heartbeat` is the task's heartbeat handle from the driver: the handler's
    /// `ctx.heartbeat()` records through it, and the context shares its
    /// cancellation token.
    ///
    /// # Errors
    ///
    /// Fails if no handler is registered for the task type or the handler
    /// returns an error. A [`TaskError::Application`](crate::error::TaskError::Application)
    /// keeps its error type and non-retryable flag.
    // The error type is sdk-core's, returned to its driver unchanged, so its size is not
    // this crate's to reduce.
    #[allow(clippy::result_large_err)]
    pub async fn execute_task(
        &self,
        task: TaskExecutionTask,
        heartbeat: orcher_sdk_core::poller::TaskHeartbeat,
    ) -> orcher_sdk_core::Result<Vec<u8>> {
        let task_id = task.task_id.clone();
        let task_type = task.task_type.clone();

        self.tasks_executed.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            task_id = %task_id,
            task_type = %task_type,
            "Executing task in sdk-rust runtime"
        );

        let _permit = self.task_semaphore.acquire().await.map_err(|e| {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
            orcher_sdk_core::Error::internal(format!("Failed to acquire task permit: {}", e))
        })?;

        let handler = {
            let handlers = self.task_handlers.read().await;
            handlers.get(&task_type).cloned()
        };

        let handler = match handler {
            Some(h) => h,
            None => {
                self.tasks_failed.fetch_add(1, Ordering::Relaxed);
                return Err(orcher_sdk_core::Error::internal(format!(
                    "No handler registered for task type: {}",
                    task_type
                )));
            }
        };

        // The heartbeat timeout comes from the poll response, so that
        // `ctx.heartbeat_timeout()` reports the interval the server judges the
        // task by. A handler needs it to pace its heartbeats; missing that
        // interval gets a healthy task killed.
        let ctx = TaskContext::with_heartbeat(
            task.execution.workflow_id.clone(),
            task.execution.run_id.clone(),
            task.task_id.clone(),
            task.attempt,
            task.heartbeat_timeout,
            heartbeat,
        );

        let mut input = Payload::new_data(task.input.clone());
        if let Some(ref codec) = self.config.codec_chain {
            use orcher_sdk_core::codec::PayloadCodec;
            match codec.decode(&input) {
                Ok(decoded) => {
                    input = decoded;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode task input with codec chain");
                }
            }
        }
        match handler(ctx, input).await {
            Ok(output) => {
                self.tasks_succeeded.fetch_add(1, Ordering::Relaxed);
                *self.last_success_time.lock().unwrap() = Some(Instant::now());
                Ok(output.data)
            }
            Err(e) => {
                self.tasks_failed.fetch_add(1, Ordering::Relaxed);
                Err(task_failure(e))
            }
        }
    }

    async fn inject_step_results(
        &self,
        ctx: &WorkflowContext,
        jobs: &[RequestJob],
        mut times: JournalTimes,
    ) {
        // Jobs arrive in journal order. The position is recorded so that a wait
        // with a timeout can tell whether its event or its deadline came first.
        for (position, job) in jobs.iter().enumerate() {
            if let Some(key) = result_key(job) {
                if let Some(at_ms) = times.resolved_at.get(&key) {
                    ctx.record_resolved_at(key, *at_ms);
                }
            }
            match job {
                RequestJob::CompleteTask(complete_task) => {
                    if let orcher_sdk_core::bridge::TaskExecutionResult::Success { output } =
                        &complete_task.result
                    {
                        ctx.inject_step_result(complete_task.task_id.clone(), output.data.clone());
                    }
                }
                RequestJob::CompleteStep(complete_step) => {
                    if let Some(failure) = &complete_step.failure {
                        // A terminal task or step failure reported by the server. Inject a
                        // sentinel keyed by step_name so that execute_task/execute decode it on
                        // replay and raise a catchable error, as the ChildWorkflowFailed arm
                        // does. Dropping the failure would leave replay with no result, and the
                        // task would be re-executed forever.
                        let error_bytes = serde_json::to_vec(&serde_json::json!({
                            crate::error::TASK_FAILED_SENTINEL_KEY: true,
                            "message": failure.message,
                            "error_type": failure.failure_type,
                            // How many times the task actually ran. The decode side falls back
                            // to 1 when this is missing, which would report a task that
                            // exhausted its retries as having run once.
                            "attempts": complete_step.execution_attempt,
                        }))
                        .unwrap_or_default();
                        ctx.inject_step_result(complete_step.step_name.clone(), error_bytes);
                    } else {
                        ctx.inject_step_result(
                            complete_step.step_name.clone(),
                            complete_step.result.clone(),
                        );
                    }
                }
                RequestJob::HandleEvent(event_job) => {
                    // The n-th event of a name is the n-th journaled under it.
                    let at_ms = times
                        .events
                        .get_mut(&event_job.event_name)
                        .and_then(|times| times.pop_front());
                    ctx.buffer_journaled_event(
                        event_job.event_name.clone(),
                        event_job.payload.data.clone(),
                        Some(position),
                        at_ms,
                    );
                }
                RequestJob::ChildWorkflowCompleted(job) => {
                    ctx.inject_step_result(
                        format!("child:{}", job.workflow_id),
                        job.result.data.clone(),
                    );
                }
                RequestJob::ChildWorkflowFailed(job) => {
                    inject_child_failure(ctx, &job.workflow_id, &job.failure.message);
                }
                // A child that did not complete failed as far as its parent is
                // concerned: `result()` raises `ChildWorkflowFailed` with the reason.
                // Without these the parent would wait on the child forever.
                RequestJob::ChildWorkflowCanceled(job) => {
                    inject_child_failure(ctx, &job.workflow_id, "child workflow was canceled");
                }
                RequestJob::ChildWorkflowTerminated(job) => {
                    let message = if job.reason.is_empty() {
                        "child workflow was terminated".to_string()
                    } else {
                        format!("child workflow was terminated: {}", job.reason)
                    };
                    inject_child_failure(ctx, &job.workflow_id, &message);
                }
                RequestJob::ChildWorkflowTimedOut(job) => {
                    inject_child_failure(ctx, &job.workflow_id, "child workflow timed out");
                }
                RequestJob::FireTimer(job) => {
                    // A durable timer fired. Mark it, keyed by the user-facing timer id,
                    // so sleep()/sleep_with_id() return on replay instead of
                    // emitting StartTimer again.
                    ctx.mark_timer_fired_at(job.timer_id.clone(), Some(position));
                }
                _ => {}
            }
        }
    }

    /// Answers the activation's queries and updates.
    ///
    /// Must run after the workflow handler, which registers the query and
    /// update handlers on the context.
    async fn process_query_and_update_jobs(
        &self,
        ctx: &WorkflowContext,
        jobs: &[RequestJob],
    ) -> (Vec<QueryResponse>, Vec<UpdateResponse>) {
        let mut query_responses = Vec::new();
        let mut update_results = Vec::new();

        for job in jobs {
            match job {
                RequestJob::ProcessQuery(query_job) => {
                    let response = match ctx.handle_query_request(&query_job.query_type) {
                        Ok(output) => QueryResponse {
                            query_id: query_job.query_id.clone(),
                            result: QueryResult::Success {
                                output: Payload::new_data(output),
                            },
                        },
                        Err(e) => QueryResponse {
                            query_id: query_job.query_id.clone(),
                            result: QueryResult::Failed {
                                message: e.to_string(),
                                details: None,
                            },
                        },
                    };
                    query_responses.push(response);
                }
                RequestJob::UpdateState(update_job) => {
                    let response = match ctx
                        .handle_update_request(
                            &update_job.update_name,
                            update_job.payload.data.clone(),
                        )
                        .await
                    {
                        Ok(output) => UpdateResponse {
                            update_id: update_job.update_id.clone(),
                            result: UpdateResult::Completed {
                                output: Payload::new_data(output),
                            },
                        },
                        Err(e) => UpdateResponse {
                            update_id: update_job.update_id.clone(),
                            result: UpdateResult::Rejected {
                                message: e.to_string(),
                            },
                        },
                    };
                    update_results.push(response);
                }
                _ => {} // Other jobs are handled before the handler runs.
            }
        }

        (query_responses, update_results)
    }

    async fn get_or_extract_input(&self, run_id: &str, jobs: &[RequestJob]) -> Result<Payload> {
        // A later activation may not carry the start job, so prefer the cache.
        {
            let cache = self.input_cache.read().await;
            if let Some(payload) = cache.get(run_id) {
                return Ok(payload.clone());
            }
        }

        for job in jobs {
            if let RequestJob::StartWorkflow(start) = job {
                let mut payload = Payload::from_json(&start.input)
                    .unwrap_or_else(|_| Payload::new_data(Vec::new()));

                // On a decode failure, log and pass the input on undecoded.
                if let Some(ref codec) = self.config.codec_chain {
                    use orcher_sdk_core::codec::PayloadCodec;
                    match codec.decode(&payload) {
                        Ok(decoded) => {
                            payload = decoded;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to decode workflow input with codec chain");
                        }
                    }
                }

                self.input_cache
                    .write()
                    .await
                    .insert(run_id.to_string(), payload.clone());

                return Ok(payload);
            }
        }

        Err(Error::Worker(
            crate::error::WorkerError::InvalidConfiguration(
                "No workflow input available".to_string(),
            ),
        ))
    }

    async fn cleanup_input_cache(&self, run_id: &str) {
        self.input_cache.write().await.remove(run_id);
    }

    fn convert_command(
        &self,
        cmd: WorkflowCommand,
        default_task_queue: &str,
    ) -> Result<BridgeCommand> {
        use crate::workflow::CommandOrphanPolicy;
        use orcher_sdk_core::bridge::OrphanPolicy as BridgeOrphanPolicy;

        match cmd {
            WorkflowCommand::RecordStepResult(cmd) => {
                let failure = cmd
                    .failure
                    .as_ref()
                    .map(|f| orcher_sdk_core::bridge::StepFailure {
                        message: f.message.clone(),
                        stack_trace: f.stack_trace.clone(),
                        source: f.source.clone(),
                        failure_type: f.failure_type.clone(),
                    });

                Ok(BridgeCommand::RecordStepResult(
                    orcher_sdk_core::bridge::RecordStepResultCommand {
                        step_name: cmd.step_name,
                        step_type: cmd.step_type,
                        result: cmd.result,
                        failure,
                        execution_attempt: cmd.execution_attempt,
                    },
                ))
            }

            WorkflowCommand::ScheduleTask(task) => {
                let retry_policy = task.retry_policy.as_ref().map(|rp| BridgeRetryPolicy {
                    max_attempts: rp.max_attempts,
                    initial_interval: rp.initial_interval,
                    max_interval: rp.max_interval,
                    backoff_coefficient: rp.backoff_coefficient,
                    non_retryable_errors: rp.non_retryable_errors.clone(),
                });

                let headers = task
                    .headers
                    .into_iter()
                    .map(|(k, v)| (k, Payload::new_data(v)))
                    .collect();

                Ok(BridgeCommand::ScheduleTask(ScheduleTaskCommand {
                    sequence: task.sequence as u32,
                    task_id: task.task_id,
                    task_type: task.task_type,
                    task_queue: if task.task_queue.is_empty() {
                        default_task_queue.to_string()
                    } else {
                        task.task_queue
                    },
                    input: vec![Payload::new_data(task.input)],
                    timeout: task.timeout,
                    heartbeat_timeout: task.heartbeat_timeout,
                    queue_timeout: task.queue_timeout,
                    retry_policy,
                    headers,
                }))
            }

            WorkflowCommand::StartTimer(timer) => {
                Ok(BridgeCommand::StartTimer(StartTimerCommand {
                    sequence: timer.sequence as u32,
                    timer_id: timer.timer_id,
                    duration: timer.duration,
                }))
            }

            WorkflowCommand::CancelTimer(cancel) => Ok(BridgeCommand::CancelTimer(
                orcher_sdk_core::bridge::CancelTimerCommand {
                    sequence: cancel.sequence as u32,
                    timer_id: cancel.timer_id,
                },
            )),

            WorkflowCommand::CompleteWorkflow(complete) => {
                Ok(BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                    result: Payload::new_data(complete.result),
                }))
            }

            WorkflowCommand::FailWorkflow(fail) => {
                Ok(BridgeCommand::FailWorkflow(FailWorkflowCommand {
                    message: fail.message,
                    details: fail.details.map(Payload::new_data),
                    error_type: String::new(),
                }))
            }

            WorkflowCommand::StartChildWorkflow(child) => {
                let orphan_policy = match child.orphan_policy {
                    CommandOrphanPolicy::Terminate => BridgeOrphanPolicy::Terminate,
                    CommandOrphanPolicy::Abandon => BridgeOrphanPolicy::Abandon,
                    CommandOrphanPolicy::Cancel => BridgeOrphanPolicy::Cancel,
                };

                Ok(BridgeCommand::StartChildWorkflow(
                    orcher_sdk_core::bridge::StartChildWorkflowCommand {
                        sequence: child.sequence as u32,
                        workflow_id: child.workflow_id,
                        workflow_type: child.workflow_type,
                        task_queue: if child.task_queue.is_empty() {
                            default_task_queue.to_string()
                        } else {
                            child.task_queue
                        },
                        input: vec![Payload::new_data(child.input)],
                        timeout: child.timeout,
                        orphan_policy,
                    },
                ))
            }

            WorkflowCommand::SendEvent(event) => Ok(BridgeCommand::SendEvent(
                orcher_sdk_core::bridge::SendEventCommand {
                    workflow_id: event.workflow_id,
                    run_id: None,
                    event_name: event.event_name,
                    payload: Payload::new_data(event.data),
                    headers: vec![],
                },
            )),

            // The engine identifies the child by the id its parent gave it; the run id
            // is left empty because the child's run changes when it is retried.
            WorkflowCommand::CancelChildWorkflow(cancel) => Ok(BridgeCommand::CancelChildWorkflow(
                orcher_sdk_core::bridge::CancelChildWorkflowCommand {
                    workflow_id: cancel.workflow_id,
                    run_id: String::new(),
                },
            )),

            WorkflowCommand::WaitForEvent(cmd) => {
                Ok(BridgeCommand::WaitForEvent(WaitForEventCommand {
                    step_id: format!("event_{}_{}", cmd.event_name, cmd.sequence),
                    event_name: cmd.event_name,
                    timeout_ms: cmd.timeout.map(|d| d.as_millis() as i64),
                }))
            }

            WorkflowCommand::RestartFresh(restart) => Ok(BridgeCommand::RestartFresh(
                orcher_sdk_core::bridge::RestartFreshCommand {
                    workflow_type: restart.workflow_type.unwrap_or_default(),
                    task_queue: restart.task_queue,
                    input: vec![Payload::new_data(restart.input)],
                    timeout: restart.timeout,
                },
            )),
        }
    }
}

/// When the engine journaled what an activation can hand its workflow, read
/// from the journal itself: the jobs derived from it carry no times.
#[derive(Debug, Default)]
struct JournalTimes {
    /// When the workflow's start was journaled, ms since the epoch.
    started_at_ms: Option<i64>,
    /// When each result was journaled, keyed as the context holds it: a
    /// step's name, `timer:{id}`, `child:{workflow id}`.
    resolved_at: HashMap<String, i64>,
    /// When each event was journaled, per name, in journal order.
    events: HashMap<String, std::collections::VecDeque<i64>>,
}

impl JournalTimes {
    fn read(journal: &[JournalEntry]) -> Self {
        let mut times = Self::default();
        for entry in journal {
            let Some(at_ms) = entry.timestamp.as_ref().map(|t| {
                t.seconds
                    .saturating_mul(1000)
                    .saturating_add(i64::from(t.nanos) / 1_000_000)
            }) else {
                continue;
            };
            if entry.entry_type == EntryType::WorkflowExecutionStarted as i32 {
                times.started_at_ms.get_or_insert(at_ms);
            }
            let key = match &entry.attributes {
                Some(Attributes::StepCompleted(a)) => a.step_name.clone(),
                Some(Attributes::TimerFired(a)) => format!("timer:{}", a.timer_id),
                Some(Attributes::ChildWorkflowExecutionCompleted(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionFailed(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionCanceled(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionTerminated(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::ChildWorkflowExecutionTimedOut(a)) => {
                    format!("child:{}", a.workflow_id)
                }
                Some(Attributes::EventReceived(a)) => {
                    times
                        .events
                        .entry(a.event_name.clone())
                        .or_default()
                        .push_back(at_ms);
                    continue;
                }
                _ => continue,
            };
            times.resolved_at.entry(key).or_insert(at_ms);
        }
        times
    }
}

/// Records that a child ended without a result, so `result()` raises
/// `ChildWorkflowFailed` with `message` on replay.
fn inject_child_failure(ctx: &WorkflowContext, workflow_id: &str, message: &str) {
    let error_bytes = serde_json::to_vec(&serde_json::json!({
        "__orcher_child_failed__": true,
        "message": message
    }))
    .unwrap_or_default();
    ctx.inject_step_result(format!("child:{workflow_id}"), error_bytes);
}

/// The key a job's result is held under in the workflow context.
fn result_key(job: &RequestJob) -> Option<String> {
    match job {
        RequestJob::CompleteTask(job) => Some(job.task_id.clone()),
        RequestJob::CompleteStep(job) => Some(job.step_name.clone()),
        RequestJob::ChildWorkflowCompleted(job) => Some(format!("child:{}", job.workflow_id)),
        RequestJob::ChildWorkflowFailed(job) => Some(format!("child:{}", job.workflow_id)),
        RequestJob::ChildWorkflowCanceled(job) => Some(format!("child:{}", job.workflow_id)),
        RequestJob::ChildWorkflowTerminated(job) => Some(format!("child:{}", job.workflow_id)),
        RequestJob::ChildWorkflowTimedOut(job) => Some(format!("child:{}", job.workflow_id)),
        RequestJob::FireTimer(job) => Some(format!("timer:{}", job.timer_id)),
        _ => None,
    }
}

/// Converts a task's error into the failure reported to the server.
///
/// The server decides whether to retry from the error type it receives. A
/// failure the task named with
/// [`TaskError::Application`](crate::error::TaskError::Application) is sent
/// under that type, with its non-retryable flag. Any other error has no type
/// a retry policy could list, and is reported as an internal error.
fn task_failure(error: crate::Error) -> orcher_sdk_core::Error {
    match error {
        crate::Error::Task(crate::error::TaskError::Application {
            error_type,
            message,
            non_retryable,
        }) => orcher_sdk_core::TaskFailure::new(message)
            .with_type(error_type)
            .with_non_retryable(non_retryable)
            .into(),
        other => orcher_sdk_core::Error::internal(other.to_string()),
    }
}

/// The failure reported to the core for a workflow handler's error.
///
/// Non-determinism is reported as such, so the core fails the execution as
/// non-deterministic and not retryable instead of as an ordinary workflow
/// error: the same code against the same journal would fail the same way.
fn workflow_failure(error: &Error) -> orcher_sdk_core::bridge::ExecutionError {
    use orcher_sdk_core::bridge::{ExecutionError, ExecutionErrorType};
    match error {
        Error::Workflow(crate::error::WorkflowError::NonDeterministic(_)) => {
            ExecutionError::non_determinism(error.to_string(), None)
        }
        _ => ExecutionError {
            message: error.to_string(),
            error_type: ExecutionErrorType::WorkflowCode,
            details: None,
            retryable: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TaskError;

    #[test]
    fn a_named_task_failure_keeps_its_type_and_mark() {
        let reported = task_failure(TaskError::non_retryable("CardDeclined", "declined").into());
        let failure = reported.task_failure().expect("a typed failure");
        assert_eq!(failure.error_type.as_deref(), Some("CardDeclined"));
        assert!(failure.non_retryable);
        assert_eq!(reported.to_string(), "declined");

        let reported = task_failure(TaskError::application("Throttled", "slow down").into());
        let failure = reported.task_failure().expect("a typed failure");
        assert_eq!(failure.error_type.as_deref(), Some("Throttled"));
        assert!(!failure.non_retryable);

        let reported = task_failure(TaskError::execution_failed("boom").into());
        assert!(
            reported.task_failure().is_none(),
            "an unnamed error has no type"
        );
    }

    #[test]
    fn a_non_deterministic_workflow_is_reported_as_non_determinism() {
        use orcher_sdk_core::bridge::ExecutionErrorType;

        let reported = workflow_failure(&Error::Workflow(
            crate::error::WorkflowError::non_deterministic("step 3 changed"),
        ));
        assert_eq!(reported.error_type, ExecutionErrorType::NonDeterminism);
        assert!(!reported.retryable);
        assert!(reported.message.contains("step 3 changed"));

        let reported = workflow_failure(&Error::Workflow(crate::error::WorkflowError::Panic(
            "boom".into(),
        )));
        assert_eq!(reported.error_type, ExecutionErrorType::WorkflowCode);
    }

    #[test]
    fn test_runtime_config_default() {
        let config = RuntimeConfig::default();
        assert_eq!(config.max_concurrent_workflows, 100);
        assert_eq!(config.max_concurrent_tasks, 200);
        assert_eq!(config.default_task_queue, "default");
    }

    #[tokio::test]
    async fn test_runtime_handler_registration() {
        let runtime = ExecutionRuntime::new(RuntimeConfig::default());

        let handler: WorkflowHandlerFn =
            Arc::new(|_ctx, _input| Box::pin(async move { Ok(Payload::new_data(Vec::new())) }));

        runtime
            .register_workflow_handler("TestWorkflow", handler)
            .await;

        let handlers = runtime.workflow_handlers.read().await;
        assert!(handlers.contains_key("TestWorkflow"));
    }

    #[tokio::test]
    async fn test_runtime_task_handler_registration() {
        let runtime = ExecutionRuntime::new(RuntimeConfig::default());

        let handler: TaskHandlerFn =
            Arc::new(|_ctx, _input| Box::pin(async move { Ok(Payload::new_data(Vec::new())) }));

        runtime.register_task_handler("TestTask", handler).await;

        let handlers = runtime.task_handlers.read().await;
        assert!(handlers.contains_key("TestTask"));
    }
}
