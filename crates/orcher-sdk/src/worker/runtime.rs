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
use crate::workflow::ReachedStep;
use crate::workflow::{WorkflowCommand, WorkflowContext};
use crate::TaskContext;

use orcher_sdk_core::bridge::{
    CancelWorkflowCommand, Command as BridgeCommand, CompleteWorkflowCommand, ExecutionResult,
    FailWorkflowCommand, QueryResponse, QueryResult, ScheduleTaskCommand, StartTimerCommand,
    TaskRetryPolicy as BridgeRetryPolicy, UpdateResponse, UpdateResult, WaitForEventCommand,
};
use orcher_sdk_core::poller::{TaskExecutionTask, WorkflowExecutionTask};
use orcher_sdk_core::proto::host::v1::{task_outcome, TaskAssignment, TaskOutcome};
use orcher_sdk_core::proto::kernel::v1::{Failure as KernelFailure, Payload as KernelPayload};

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

    /// Runs one activation as the kernel protocol carries it; returns what
    /// it produced, with the steps the code reached and their kinds. An
    /// activation that cannot be read is an error, for the protocol error it
    /// answers with.
    pub(crate) async fn execute_activation(
        &self,
        activation: orcher_sdk_core::proto::kernel::v1::RunActivation,
    ) -> std::result::Result<(ExecutionResult, Vec<ReachedStep>), String> {
        let loaded = crate::worker::activation::load(activation, &self.config.namespace)?;
        let execution = loaded.ctx.execution().clone();
        let run_id = execution.run_id.clone();
        self.workflows_executed.fetch_add(1, Ordering::Relaxed);

        let _permit = self
            .workflow_semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire execution permit: {e}"))?;
        let handler = {
            let handlers = self.workflow_handlers.read().await;
            handlers.get(&execution.workflow_type).cloned()
        };
        let Some(handler) = handler else {
            tracing::error!(
                workflow_type = %execution.workflow_type,
                "No handler registered for workflow type"
            );
            return Ok((
                ExecutionResult::failed(
                    run_id,
                    orcher_sdk_core::bridge::ExecutionError {
                        message: format!(
                            "No handler registered for workflow type: {}",
                            execution.workflow_type
                        ),
                        error_type: orcher_sdk_core::bridge::ExecutionErrorType::WorkflowCode,
                        details: None,
                        // Not raised as non-retryable by the workflow code.
                        retryable: true,
                    },
                ),
                Vec::new(),
            ));
        };
        let input = self.decode_input(loaded.input.unwrap_or(serde_json::Value::Null));
        Ok(self
            .run_handler(loaded.ctx, input, handler, &loaded.asked)
            .await)
    }

    /// The run's input as a payload, decoded by the codec chain when one is
    /// set; on a decode failure, logged and passed on undecoded.
    fn decode_input(&self, input: serde_json::Value) -> Payload {
        let mut payload =
            Payload::from_json(&input).unwrap_or_else(|_| Payload::new_data(Vec::new()));
        if let Some(ref codec) = self.config.codec_chain {
            use orcher_sdk_core::codec::PayloadCodec;
            match codec.decode(&payload) {
                Ok(decoded) => payload = decoded,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to decode workflow input with codec chain");
                }
            }
        }
        payload
    }

    /// Runs the workflow's code once its context holds what history
    /// resolved, answers the queries and updates asked of it, and returns
    /// what the activation produced, with the steps the code reached.
    pub(crate) async fn run_handler(
        &self,
        ctx: WorkflowContext,
        input: Payload,
        handler: WorkflowHandlerFn,
        asked: &[Asked],
    ) -> (ExecutionResult, Vec<ReachedStep>) {
        let execution = ctx.execution().clone();
        let (run_id, workflow_id, task_queue) = (
            execution.run_id,
            execution.workflow_id.as_str(),
            execution.task_queue.as_str(),
        );
        let handler_result = handler(ctx.clone(), input).await;

        // Queries and updates run after the handler, because the handler is
        // what registers their handlers on the context.
        let (query_responses, update_results) = self.answer_asked(&ctx, asked).await;
        // The steps the code reached, which sdk-core checks against the
        // journal to tell code that no longer replays the run.
        let reached = ctx.take_reached_steps();
        let reached_steps = reached.iter().map(|step| step.id.clone()).collect();

        let mut result = match handler_result {
            Ok(output) => {
                let r = self.handle_workflow_completion(
                    Ending::Completed(output),
                    ctx,
                    run_id,
                    task_queue,
                    query_responses,
                    update_results,
                );
                self.workflows_succeeded.fetch_add(1, Ordering::Relaxed);
                *self.last_success_time.lock().unwrap() = Some(Instant::now());
                r
            }
            // The cancellation request, delivered to the workflow and not caught, ends
            // it as cancelled, with whatever cleanup it issued before giving up. A
            // `Canceled` error the workflow raised without having been asked is a
            // failure like any other, below.
            Err(Error::Workflow(crate::error::WorkflowError::Canceled))
                if ctx.is_cancel_requested() =>
            {
                tracing::info!(workflow_id = %workflow_id, "Workflow ended as cancelled");
                self.handle_workflow_completion(
                    Ending::Cancelled,
                    ctx,
                    run_id,
                    task_queue,
                    query_responses,
                    update_results,
                )
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
                    task_queue,
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
        (result, reached)
    }

    fn handle_workflow_completion(
        &self,
        ending: Ending,
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
                            // Not raised as non-retryable by the workflow code.
                            retryable: true,
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
            bridge_commands.push(match ending {
                Ending::Completed(output) => {
                    BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                        result: Payload::new_data(output.data),
                    })
                }
                Ending::Cancelled => {
                    BridgeCommand::CancelWorkflowExecution(CancelWorkflowCommand { details: None })
                }
            });
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
                    // Not raised as non-retryable by the workflow code.
                    retryable: true,
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
                            // Not raised as non-retryable by the workflow code.
                            retryable: true,
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

    /// The workflow types registered.
    pub(crate) async fn workflow_types(&self) -> Vec<String> {
        self.workflow_handlers
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// The task types registered.
    pub(crate) async fn task_types(&self) -> Vec<String> {
        self.task_handlers.read().await.keys().cloned().collect()
    }

    /// Runs a task the worker host handed over and returns its outcome: its
    /// value, or its failure with the type and retryability the task gave.
    /// Heartbeats the task records, and its cancellation, go through `host`.
    pub async fn execute_assignment(
        &self,
        assignment: TaskAssignment,
        host: &orcher_sdk_host::Host,
    ) -> TaskOutcome {
        let token = assignment.token.clone();
        let run = assignment.run.clone().unwrap_or_default();
        let ctx = TaskContext::with_host(
            run.workflow_id,
            run.run_id,
            assignment.task_id.clone(),
            assignment.attempt as i32,
            assignment
                .heartbeat_timeout
                .as_ref()
                .map(|d| std::time::Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32)),
            host.clone(),
            token.clone(),
        );
        let input = assignment
            .input
            .into_iter()
            .next()
            .map(|payload| payload.data)
            .unwrap_or_default();
        let result = match self.run_task(ctx, &assignment.task_type, input).await {
            Ok(data) => task_outcome::Result::Value(KernelPayload {
                data,
                ..Default::default()
            }),
            Err(error) => task_outcome::Result::Failure(reported_failure(&error)),
        };
        TaskOutcome {
            token,
            result: Some(result),
            ..Default::default()
        }
    }

    /// Runs a task's handler with its context and input.
    // The error is sdk-core's own, reported as it is; its size is not this
    // crate's to change.
    #[allow(clippy::result_large_err)]
    async fn run_task(
        &self,
        ctx: TaskContext,
        task_type: &str,
        input: Vec<u8>,
    ) -> orcher_sdk_core::Result<Vec<u8>> {
        self.tasks_executed.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            task_id = %ctx.task_id(),
            task_type = %task_type,
            "Executing task in sdk-rust runtime"
        );

        let _permit = self.task_semaphore.acquire().await.map_err(|e| {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
            orcher_sdk_core::Error::internal(format!("Failed to acquire task permit: {}", e))
        })?;

        let handler = {
            let handlers = self.task_handlers.read().await;
            handlers.get(task_type).cloned()
        };
        let Some(handler) = handler else {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
            return Err(orcher_sdk_core::Error::internal(format!(
                "No handler registered for task type: {}",
                task_type
            )));
        };

        let mut input = Payload::new_data(input);
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

    /// Answers the activation's updates, then its queries, so a query sees
    /// what an update in the same activation changed.
    ///
    /// Must run after the workflow handler, which registers the query and
    /// update handlers on the context.
    async fn answer_asked(
        &self,
        ctx: &WorkflowContext,
        asked: &[Asked],
    ) -> (Vec<QueryResponse>, Vec<UpdateResponse>) {
        let mut query_responses = Vec::new();
        let mut update_results = Vec::new();

        for asked in asked {
            match asked {
                Asked::Query { id, name, args } => {
                    let response = match ctx.handle_query_request(name, args) {
                        Ok(output) => QueryResponse {
                            query_id: id.clone(),
                            result: QueryResult::Success {
                                output: Payload::new_data(output),
                            },
                        },
                        Err(e) => QueryResponse {
                            query_id: id.clone(),
                            result: QueryResult::Failed {
                                message: e.to_string(),
                                details: None,
                            },
                        },
                    };
                    query_responses.push(response);
                }
                Asked::Update { id, name, args } => {
                    let response = match ctx.handle_update_request(name, args.clone()).await {
                        Ok(output) => UpdateResponse {
                            update_id: id.clone(),
                            result: UpdateResult::Completed {
                                output: Payload::new_data(output),
                            },
                        },
                        Err(e) => UpdateResponse {
                            update_id: id.clone(),
                            result: UpdateResult::Rejected {
                                message: e.to_string(),
                            },
                        },
                    };
                    update_results.push(response);
                }
            }
        }

        (query_responses, update_results)
    }

    pub(crate) fn convert_command(
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
                        retry_policy: child.retry_policy.as_ref().map(workflow_retry_policy),
                        ..Default::default()
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
                    retry_policy: restart.retry_policy.as_ref().map(workflow_retry_policy),
                    ..Default::default()
                },
            )),
        }
    }
}

/// A child's or a fresh run's retry policy, sent as a workflow start sends
/// one: `max_attempts` as given (the first run included, 0 unlimited), so a
/// policy left at its default count sends 3.
fn workflow_retry_policy(
    policy: &crate::task::RetryPolicy,
) -> orcher_sdk_core::bridge::WorkflowRetryPolicy {
    orcher_sdk_core::bridge::WorkflowRetryPolicy {
        max_attempts: policy.max_attempts,
        initial_interval: Some(policy.initial_interval),
        max_interval: Some(policy.max_interval),
        backoff_coefficient: policy.backoff_coefficient,
        non_retryable_errors: policy.non_retryable_errors.clone(),
    }
}

/// Records that a child ended without a result, so `result()` raises
/// `ChildWorkflowFailed` with `message` on replay.
pub(crate) fn inject_child_failure(ctx: &WorkflowContext, workflow_id: &str, message: &str) {
    let error_bytes = serde_json::to_vec(&serde_json::json!({
        "__orcher_child_failed__": true,
        "message": message
    }))
    .unwrap_or_default();
    ctx.inject_step_result(format!("child:{workflow_id}"), error_bytes);
}

/// A query or update asked of the workflow in an activation.
pub(crate) enum Asked {
    /// A query: read-only, answered from the workflow's state.
    Query {
        id: String,
        name: String,
        /// Its arguments, as the one JSON payload the client sends.
        args: Vec<u8>,
    },
    /// An update: may change the workflow's state.
    Update {
        id: String,
        name: String,
        args: Vec<u8>,
    },
}

/// How a workflow's code ended, for the terminal command it gets.
enum Ending {
    /// It returned this output.
    Completed(Payload),
    /// It gave up on a cancellation request it had been told of.
    Cancelled,
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

/// A handler's error as the failure the host reports: the type and
/// retryability a [`TaskError::Application`](crate::error::TaskError::Application)
/// names, or only the message for any other error.
pub(crate) fn failure_of(error: crate::Error) -> KernelFailure {
    reported_failure(&task_failure(error))
}

fn reported_failure(error: &orcher_sdk_core::Error) -> KernelFailure {
    match error.task_failure() {
        Some(failure) => KernelFailure {
            message: failure.message.clone(),
            error_type: failure.error_type.clone().unwrap_or_default(),
            non_retryable: failure.non_retryable,
            ..Default::default()
        },
        None => KernelFailure {
            message: error.to_string(),
            ..Default::default()
        },
    }
}

/// The failure reported to the core for a workflow handler's error.
///
/// Non-determinism is reported as such, so the core fails the execution as
/// non-deterministic and not retryable instead of as an ordinary workflow
/// error: the same code against the same journal would fail the same way.
///
/// Any other error is retryable, so the workflow's retry policy decides,
/// unless the workflow code raised it as non-retryable the way task code
/// does: a [`TaskError::non_retryable`](crate::error::TaskError::non_retryable).
/// A task's failure the workflow passes on is the workflow's own failure,
/// retryable like any other.
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
            retryable: !matches!(
                error,
                Error::Task(crate::error::TaskError::Application {
                    non_retryable: true,
                    ..
                })
            ),
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

    /// A workflow's failure is non-retryable only when its code raised it as
    /// non-retryable, the way task code does; any other error is left to the
    /// workflow's retry policy, a task's failure passed on included.
    #[test]
    fn a_workflow_failure_is_non_retryable_only_when_raised_so() {
        let raised = workflow_failure(&TaskError::non_retryable("OutOfStock", "gone").into());
        assert!(!raised.retryable);
        assert!(raised.message.contains("gone"));

        for error in [
            TaskError::application("Throttled", "slow down").into(),
            TaskError::execution_failed("boom").into(),
            Error::Workflow(crate::error::WorkflowError::Panic("boom".into())),
            Error::Workflow(crate::error::WorkflowError::task_failed(
                "charge", 3, "declined",
            )),
        ] {
            let reported = workflow_failure(&error);
            assert!(reported.retryable, "{error:?} was reported non-retryable");
        }
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
