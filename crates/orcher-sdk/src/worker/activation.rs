//! Workflow activations as the kernel protocol carries them.
//!
//! The worker host hands a workflow activation over as a [`RunActivation`]:
//! the run's history already resolved into the outcomes its code observes.
//! [`ExecutionRuntime::run_activation`] loads those into a fresh
//! [`WorkflowContext`], runs the workflow's code, and answers with a
//! [`RunActivationResult`]: every durable call the code made, in issue order,
//! those answered from history marked satisfied, with the queries and
//! updates answered.

use std::collections::HashMap;
use std::time::Duration;

use orcher_sdk_core::bridge::{
    Command as BridgeCommand, ExecutionResult, OrphanPolicy, QueryResult, TaskRetryPolicy,
    UpdateResult,
};
use orcher_sdk_core::proto::kernel::v1::{
    self as kernel, command, outcome, query_answer, resolution, run_activation_result,
    update_answer, Command, Commands, End, Failure, FailureCode, Outcome, ProtocolError,
    QueryAnswer, Resolution, RunActivation, RunActivationResult, UpdateAnswer,
};
use orcher_sdk_core::proto::prost_types;

use crate::worker::runtime::{inject_child_failure, Asked, ExecutionRuntime};
use crate::workflow::{ReachedStep, StepKind, WorkflowContext, WorkflowExecution};

/// What a [`RunActivation`] holds for the workflow's code, loaded.
pub(crate) struct Loaded {
    pub(crate) ctx: WorkflowContext,
    /// The run's input, as the JSON the client started it with.
    pub(crate) input: Option<serde_json::Value>,
    pub(crate) asked: Vec<Asked>,
}

impl ExecutionRuntime {
    /// Runs one workflow activation, as the kernel protocol carries it, and
    /// answers it.
    pub async fn run_activation(&self, activation: RunActivation) -> RunActivationResult {
        let activation_id = activation.activation_id.clone();
        match self.execute_activation(activation).await {
            Ok((result, reached)) => answer(activation_id, result, reached),
            Err(message) => RunActivationResult {
                activation_id,
                outcome: Some(run_activation_result::Outcome::ProtocolError(
                    ProtocolError {
                        message,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        }
    }
}

/// Loads an activation's resolutions into a context for its run, as the
/// history holds them.
pub(crate) fn load(activation: RunActivation, namespace: &str) -> Result<Loaded, String> {
    let run = activation
        .run
        .ok_or_else(|| "the activation names no run".to_string())?;
    let started = activation
        .resolutions
        .iter()
        .find_map(|resolution| match &resolution.what {
            Some(resolution::What::Started(started)) => Some((resolution, started)),
            _ => None,
        });
    let started_at_ms = started
        .and_then(|(resolution, started)| started.started_at.as_ref().or(resolution.at.as_ref()))
        .map(millis)
        .unwrap_or_else(|| {
            tracing::warn!(
                workflow_id = %run.workflow_id,
                "The activation records no start time; workflow time starts now"
            );
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });
    let input = started
        .and_then(|(_, started)| started.input.first())
        .map(|payload| serde_json::from_slice(&payload.data).unwrap_or(serde_json::Value::Null));
    let execution = WorkflowExecution {
        workflow_id: run.workflow_id.clone(),
        run_id: run.run_id.clone(),
        workflow_type: run.workflow_type.clone(),
        attempt: started.map_or(1, |(_, started)| started.attempt.max(1) as i32),
        namespace: if run.namespace.is_empty() {
            namespace.to_string()
        } else {
            run.namespace.clone()
        },
        task_queue: run.task_queue.clone(),
    };
    // History holds more than the start: the code runs again over what it
    // already did.
    let replaying = activation
        .resolutions
        .iter()
        .any(|resolution| !matches!(resolution.what, Some(resolution::What::Started(_))))
        || started.is_some();
    let ctx =
        WorkflowContext::starting_at(execution, replaying, "1.0.0".to_string(), started_at_ms);

    let mut asked = Vec::new();
    for (position, resolution) in activation.resolutions.iter().enumerate() {
        load_one(&ctx, position, resolution, &mut asked);
    }
    asked.extend(activation.queries.into_iter().map(|query| {
        Asked::Query {
            id: query.query_id,
            name: query.name,
            args: query
                .args
                .into_iter()
                .next()
                .map(|payload| payload.data)
                .unwrap_or_default(),
        }
    }));
    Ok(Loaded { ctx, input, asked })
}

fn load_one(
    ctx: &WorkflowContext,
    position: usize,
    resolution: &Resolution,
    asked: &mut Vec<Asked>,
) {
    let at_ms = resolution.at.as_ref().map(millis);
    let step_id = &resolution.step_id;
    match &resolution.what {
        Some(resolution::What::Started(_)) | None => {}
        Some(resolution::What::Step(settled)) => {
            if let Some(at_ms) = at_ms {
                ctx.record_resolved_at(step_id.clone(), at_ms);
            }
            let Some(outcome) = &settled.outcome else {
                return;
            };
            match &outcome.result {
                Some(outcome::Result::Value(value)) => {
                    ctx.inject_step_result(step_id.clone(), value.data.clone());
                }
                Some(outcome::Result::Failure(failure)) => {
                    let error = crate::workflow::executable::encode_task_failure(
                        &failure.message,
                        &failure.error_type,
                        outcome.attempts.max(1) as i32,
                    );
                    ctx.inject_step_result(step_id.clone(), error);
                }
                None => {}
            }
        }
        Some(resolution::What::Child(child)) => {
            let workflow_id = &child.child_workflow_id;
            ctx.record_journaled_child(workflow_id.clone());
            if let Some(at_ms) = at_ms {
                ctx.record_resolved_at(format!("child:{workflow_id}"), at_ms);
            }
            let outcome = child.outcome.clone().unwrap_or_default();
            let message = match &outcome.result {
                Some(outcome::Result::Failure(failure)) => failure.message.clone(),
                _ => String::new(),
            };
            match End::try_from(outcome.end).unwrap_or(End::Unspecified) {
                End::Completed | End::Unspecified => {
                    if let Some(outcome::Result::Value(value)) = outcome.result {
                        ctx.inject_step_result(format!("child:{workflow_id}"), value.data);
                    }
                }
                End::Failed => inject_child_failure(ctx, workflow_id, &message),
                End::Cancelled => {
                    inject_child_failure(ctx, workflow_id, "child workflow was canceled")
                }
                End::Terminated => {
                    let message = if message.is_empty() {
                        "child workflow was terminated".to_string()
                    } else {
                        format!("child workflow was terminated: {message}")
                    };
                    inject_child_failure(ctx, workflow_id, &message);
                }
                End::TimedOut => inject_child_failure(ctx, workflow_id, "child workflow timed out"),
            }
        }
        // A child history records as started: one the code started without
        // naming keeps the id it was started under, while it still runs.
        Some(resolution::What::ChildStarted(child)) => {
            ctx.record_journaled_child(child.child_workflow_id.clone());
        }
        Some(resolution::What::Timer(_)) => {
            if let Some(at_ms) = at_ms {
                ctx.record_resolved_at(format!("timer:{step_id}"), at_ms);
            }
            ctx.mark_timer_fired_at(step_id.clone(), Some(position));
        }
        Some(resolution::What::Event(event)) => {
            ctx.buffer_journaled_event(
                event.name.clone(),
                event
                    .payload
                    .as_ref()
                    .map(|payload| payload.data.clone())
                    .unwrap_or_default(),
                Some(position),
                at_ms,
            );
        }
        Some(resolution::What::Update(update)) => asked.push(Asked::Update {
            id: update.update_id.clone(),
            name: update.name.clone(),
            args: update
                .args
                .first()
                .map(|payload| payload.data.clone())
                .unwrap_or_default(),
        }),
        Some(resolution::What::Cancel(cancel)) => {
            ctx.note_cancel_requested(cancel.requested_at.as_ref().map(millis).or(at_ms));
        }
    }
}

/// The answer to an activation: what the workflow's code produced, as the
/// kernel protocol's commands.
pub(crate) fn answer(
    activation_id: Vec<u8>,
    result: ExecutionResult,
    reached: Vec<ReachedStep>,
) -> RunActivationResult {
    let queries = result
        .query_responses
        .iter()
        .map(|response| QueryAnswer {
            query_id: response.query_id.clone(),
            answer: Some(match &response.result {
                QueryResult::Success { output } => query_answer::Answer::Value(payload(output)),
                QueryResult::Failed { message, .. } => query_answer::Answer::Failure(Failure {
                    message: message.clone(),
                    ..Default::default()
                }),
            }),
        })
        .collect();
    let updates = result
        .update_results
        .iter()
        .map(|response| UpdateAnswer {
            update_id: response.update_id.clone(),
            answer: Some(match &response.result {
                UpdateResult::Completed { output } => update_answer::Answer::Value(payload(output)),
                UpdateResult::Rejected { message } => update_answer::Answer::Rejected(Failure {
                    message: message.clone(),
                    ..Default::default()
                }),
                UpdateResult::Failed { message, .. } => update_answer::Answer::Failed(Failure {
                    message: message.clone(),
                    ..Default::default()
                }),
            }),
        })
        .collect();

    let outcome = if !result.successful {
        let error = result.error.clone().unwrap_or_else(|| {
            orcher_sdk_core::bridge::ExecutionError::workflow_error(
                "the workflow failed".to_string(),
                false,
            )
        });
        run_activation_result::Outcome::WorkflowFailed(Failure {
            message: error.message,
            non_retryable: !error.retryable,
            code: if matches!(
                error.error_type,
                orcher_sdk_core::bridge::ExecutionErrorType::NonDeterminism
            ) {
                FailureCode::NonDeterminism
            } else {
                FailureCode::Application
            } as i32,
            ..Default::default()
        })
    } else {
        match commands(&result.commands, reached) {
            Ok(commands) => run_activation_result::Outcome::Commands(Commands { commands }),
            Err(message) => run_activation_result::Outcome::ProtocolError(ProtocolError {
                message,
                ..Default::default()
            }),
        }
    };
    RunActivationResult {
        activation_id,
        outcome: Some(outcome),
        queries,
        updates,
    }
}

/// Every durable call, in issue order: the new commands as issued, with the
/// steps answered from history (reached, but not issued) marked satisfied
/// in their place among them.
fn commands(issued: &[BridgeCommand], reached: Vec<ReachedStep>) -> Result<Vec<Command>, String> {
    let issued_steps: HashMap<(StepKind, &str), usize> = issued
        .iter()
        .enumerate()
        .filter_map(|(index, command)| step_of(command).map(|step| (step, index)))
        .collect();
    let mut reached = reached.into_iter().peekable();
    let mut out = Vec::with_capacity(issued.len());
    for command in issued {
        if let Some((kind, id)) = step_of(command) {
            // The steps reached before this one that were not issued were
            // answered from history.
            while let Some(step) =
                reached.next_if(|step| (step.kind, step.id.as_str()) != (kind, id))
            {
                if !issued_steps.contains_key(&(step.kind, step.id.as_str())) {
                    out.push(satisfied(step));
                }
            }
            reached.next();
        }
        out.push(kernel_command(command)?);
    }
    for step in reached {
        if !issued_steps.contains_key(&(step.kind, step.id.as_str())) {
            out.push(satisfied(step));
        }
    }
    Ok(out)
}

/// The step a command issues, if it issues one.
fn step_of(command: &BridgeCommand) -> Option<(StepKind, &str)> {
    match command {
        BridgeCommand::ScheduleTask(task) => Some((StepKind::Task, task.task_id.as_str())),
        BridgeCommand::StartTimer(timer) => Some((StepKind::Timer, timer.timer_id.as_str())),
        BridgeCommand::StartChildWorkflow(child) => {
            Some((StepKind::Child, child.workflow_id.as_str()))
        }
        _ => None,
    }
}

/// A call answered from history: its kind and id, no payloads.
fn satisfied(step: ReachedStep) -> Command {
    let (step_id, what) = match step.kind {
        StepKind::Task => (step.id, command::What::RunTask(Default::default())),
        StepKind::Timer => (step.id, command::What::StartTimer(Default::default())),
        StepKind::Child => (
            String::new(),
            command::What::StartChild(kernel::StartChild {
                workflow_id: step.id,
                ..Default::default()
            }),
        ),
    };
    Command {
        satisfied: true,
        step_id,
        what: Some(what),
        ..Default::default()
    }
}

/// A new command, as the kernel protocol carries it.
fn kernel_command(command: &BridgeCommand) -> Result<Command, String> {
    let (seq, step_id, what) = match command {
        BridgeCommand::ScheduleTask(task) => (
            task.sequence,
            task.task_id.clone(),
            command::What::RunTask(kernel::RunTask {
                task_type: task.task_type.clone(),
                task_queue: task.task_queue.clone(),
                input: task.input.iter().map(payload).collect(),
                queue_timeout: task.queue_timeout.map(duration),
                attempt_timeout: Some(duration(task.timeout)),
                heartbeat_timeout: task.heartbeat_timeout.map(duration),
                retry_policy: task.retry_policy.as_ref().map(retry_policy),
                headers: task
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), payload(value)))
                    .collect(),
                ..Default::default()
            }),
        ),
        BridgeCommand::StartTimer(timer) => (
            timer.sequence,
            timer.timer_id.clone(),
            command::What::StartTimer(kernel::StartTimer {
                duration: Some(duration(timer.duration)),
            }),
        ),
        BridgeCommand::CancelTimer(cancel) => (
            cancel.sequence,
            String::new(),
            command::What::CancelTimer(kernel::CancelTimer {
                step_id: cancel.timer_id.clone(),
            }),
        ),
        BridgeCommand::StartChildWorkflow(child) => (
            child.sequence,
            String::new(),
            command::What::StartChild(kernel::StartChild {
                workflow_id: child.workflow_id.clone(),
                workflow_type: child.workflow_type.clone(),
                task_queue: child.task_queue.clone(),
                input: child.input.iter().map(payload).collect(),
                execution_timeout: child.timeout.map(duration),
                orphan_policy: match child.orphan_policy {
                    OrphanPolicy::Terminate => kernel::OrphanPolicy::Terminate,
                    OrphanPolicy::Cancel => kernel::OrphanPolicy::Cancel,
                    OrphanPolicy::Abandon => kernel::OrphanPolicy::Abandon,
                } as i32,
                ..Default::default()
            }),
        ),
        BridgeCommand::CancelChildWorkflow(cancel) => (
            0,
            String::new(),
            command::What::CancelChild(kernel::CancelChild {
                step_id: cancel.workflow_id.clone(),
            }),
        ),
        BridgeCommand::SendEvent(event) => (
            0,
            String::new(),
            command::What::SendEvent(kernel::SendEvent {
                workflow_id: event.workflow_id.clone(),
                run_id: event.run_id.clone().unwrap_or_default(),
                name: event.event_name.clone(),
                payload: Some(payload(&event.payload)),
                headers: event
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), payload(value)))
                    .collect(),
            }),
        ),
        BridgeCommand::RecordStepResult(record) => (
            0,
            record.step_name.clone(),
            command::What::RecordValue(kernel::RecordValue {
                outcome: Some(Outcome {
                    result: Some(match &record.failure {
                        Some(failure) => outcome::Result::Failure(Failure {
                            message: failure.message.clone(),
                            error_type: failure.failure_type.clone(),
                            ..Default::default()
                        }),
                        None => outcome::Result::Value(kernel::Payload {
                            data: record.result.clone(),
                            metadata: Default::default(),
                        }),
                    }),
                    attempts: record.execution_attempt.max(0) as u32,
                    ..Default::default()
                }),
            }),
        ),
        BridgeCommand::WaitForEvent(wait) => (
            0,
            wait.step_id.clone(),
            command::What::AwaitEvent(kernel::AwaitEvent {
                name: wait.event_name.clone(),
                timeout: wait
                    .timeout_ms
                    .map(|ms| duration(Duration::from_millis(ms.max(0) as u64))),
            }),
        ),
        BridgeCommand::CompleteWorkflow(complete) => (
            0,
            String::new(),
            command::What::CompleteRun(kernel::CompleteRun {
                result: Some(payload(&complete.result)),
            }),
        ),
        BridgeCommand::FailWorkflow(fail) => (
            0,
            String::new(),
            command::What::FailRun(kernel::FailRun {
                failure: Some(Failure {
                    message: fail.message.clone(),
                    error_type: fail.error_type.clone(),
                    details: fail
                        .details
                        .as_ref()
                        .map(|details| details.data.clone())
                        .unwrap_or_default(),
                    ..Default::default()
                }),
            }),
        ),
        BridgeCommand::CancelWorkflowExecution(cancel) => (
            0,
            String::new(),
            command::What::EndCancelled(kernel::EndCancelled {
                details: cancel
                    .details
                    .as_ref()
                    .map(|details| String::from_utf8_lossy(&details.data).into_owned())
                    .unwrap_or_default(),
            }),
        ),
        BridgeCommand::RestartFresh(restart) => (
            0,
            String::new(),
            command::What::RestartFresh(kernel::RestartFresh {
                workflow_type: restart.workflow_type.clone(),
                task_queue: restart.task_queue.clone().unwrap_or_default(),
                input: restart.input.iter().map(payload).collect(),
            }),
        ),
        other => {
            return Err(format!(
                "the workflow issued a command the kernel protocol does not carry: {other:?}"
            ))
        }
    };
    Ok(Command {
        seq: u64::from(seq),
        satisfied: false,
        step_id,
        what: Some(what),
    })
}

fn payload(payload: &orcher_sdk_core::Payload) -> kernel::Payload {
    kernel::Payload {
        data: payload.data.clone(),
        metadata: payload
            .metadata
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

fn retry_policy(policy: &TaskRetryPolicy) -> kernel::RetryPolicy {
    kernel::RetryPolicy {
        first_delay: Some(duration(policy.initial_interval)),
        delay_multiplier: policy.backoff_coefficient,
        max_delay: Some(duration(policy.max_interval)),
        max_attempts: policy.max_attempts,
        never_retry_types: policy.non_retryable_errors.clone(),
    }
}

fn duration(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

fn millis(t: &prost_types::Timestamp) -> i64 {
    t.seconds
        .saturating_mul(1000)
        .saturating_add(i64::from(t.nanos) / 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::bridge::{ScheduleTaskCommand, StartTimerCommand};

    fn task(sequence: u32, task_id: &str) -> BridgeCommand {
        BridgeCommand::ScheduleTask(ScheduleTaskCommand {
            sequence,
            task_id: task_id.into(),
            task_type: "charge".into(),
            task_queue: "orders".into(),
            input: vec![],
            timeout: Duration::from_secs(30),
            queue_timeout: None,
            heartbeat_timeout: None,
            retry_policy: None,
            headers: vec![],
        })
    }

    fn reached(kind: StepKind, id: &str) -> ReachedStep {
        ReachedStep {
            kind,
            id: id.into(),
        }
    }

    /// Calls answered from history sit where the code made them among the
    /// new ones, so every step is in issue order.
    #[test]
    fn satisfied_calls_keep_their_place_among_new_ones() {
        let issued = vec![
            task(1, "ship_1"),
            BridgeCommand::StartTimer(StartTimerCommand {
                sequence: 3,
                timer_id: "timer_3".into(),
                duration: Duration::from_secs(5),
            }),
        ];
        let steps = vec![
            reached(StepKind::Task, "charge_0"),
            reached(StepKind::Task, "ship_1"),
            reached(StepKind::Child, "order-7-child-2"),
            reached(StepKind::Timer, "timer_3"),
            reached(StepKind::Timer, "timer_4"),
        ];
        let calls = commands(&issued, steps).unwrap();
        let order: Vec<(bool, String)> = calls
            .iter()
            .map(|call| {
                let id = match call.what.as_ref().unwrap() {
                    command::What::StartChild(child) => child.workflow_id.clone(),
                    _ => call.step_id.clone(),
                };
                (call.satisfied, id)
            })
            .collect();
        assert_eq!(
            order,
            [
                (true, "charge_0".to_string()),
                (false, "ship_1".into()),
                (true, "order-7-child-2".into()),
                (false, "timer_3".into()),
                (true, "timer_4".into()),
            ]
        );
        assert_eq!(calls[1].seq, 1);
    }

    /// A child history records as started under an older SDK's default id
    /// keeps that id while it still runs: the code that started it without
    /// naming it gets the same id again, not a second child.
    #[test]
    fn a_running_child_keeps_the_id_history_holds() {
        let activation = RunActivation {
            run: Some(kernel::RunRef {
                workflow_id: "order-7".into(),
                run_id: "run-1".into(),
                workflow_type: "checkout".into(),
                ..Default::default()
            }),
            resolutions: vec![
                Resolution {
                    what: Some(resolution::What::Started(Default::default())),
                    ..Default::default()
                },
                Resolution {
                    step_id: "child_1".into(),
                    what: Some(resolution::What::ChildStarted(kernel::ChildStarted {
                        child_workflow_id: "child_1".into(),
                        child_run_id: "run-9".into(),
                    })),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let loaded = load(activation, "default").unwrap();
        assert_eq!(loaded.ctx.default_child_workflow_id(1), "child_1");
        assert_eq!(loaded.ctx.default_child_workflow_id(2), "order-7-child-2");
    }

    /// A command the kernel protocol cannot carry is a protocol error, not a
    /// command dropped on the way.
    #[test]
    fn a_command_the_protocol_cannot_carry_is_refused() {
        let issued = vec![BridgeCommand::RequestCancellation(
            orcher_sdk_core::bridge::RequestCancellationCommand {
                reason: "done".into(),
            },
        )];
        assert!(commands(&issued, vec![]).is_err());
    }
}
