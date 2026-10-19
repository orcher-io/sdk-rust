//! Workflow activations as the kernel protocol carries them.
//!
//! The worker host hands a workflow activation over as a [`RunActivation`]:
//! the run's history already resolved into the outcomes its code observes.
//! [`ExecutionRuntime::run_activation`] loads those into a fresh
//! [`WorkflowContext`], runs the workflow's code, and answers with a
//! [`RunActivationResult`]: every durable call the code made, in issue order,
//! those answered from history marked satisfied, with the queries and
//! updates answered. Code that fails answers its failure with the same
//! calls, made before it failed.

use std::collections::HashMap;
use std::time::Duration;

use orcher_sdk_core::bridge::{
    Command as BridgeCommand, ExecutionResult, OrphanPolicy, QueryResult, TaskRetryPolicy,
    UpdateResult, WorkflowRetryPolicy,
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
        // The engine counts a run's attempt from 1 (2 for the run of its
        // first workflow-level retry) and the kernel hands it over as is.
        // Engines before orcher#356 numbered the first run 0; it reads as 1,
        // so a first run is still the first attempt there. On a current engine
        // the floor never applies.
        attempt: started.map_or(1, |(_, started)| {
            i32::try_from(started.attempt).unwrap_or(i32::MAX).max(1)
        }),
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
        load_one(&ctx, order(position, resolution), resolution, &mut asked);
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

/// Where a resolution comes in the order the run observes its outcomes: the
/// sequence the kernel numbered it with. Which of two outcomes came first, a
/// wait's result or a cancellation request, is decided by it, never by their
/// times, which tie within a millisecond. A resolution the kernel did not
/// number (one built by hand) keeps its place in the list, which is the same
/// order.
fn order(position: usize, resolution: &Resolution) -> u64 {
    if resolution.sequence > 0 {
        resolution.sequence
    } else {
        position as u64 + 1
    }
}

fn load_one(ctx: &WorkflowContext, order: u64, resolution: &Resolution, asked: &mut Vec<Asked>) {
    let at_ms = resolution.at.as_ref().map(millis);
    let step_id = &resolution.step_id;
    match &resolution.what {
        Some(resolution::What::Started(_)) | None => {}
        Some(resolution::What::Step(settled)) => {
            ctx.record_resolved_order(step_id.clone(), order);
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
                        task_failure_code(failure.code, outcome.end),
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
            ctx.record_resolved_order(format!("child:{workflow_id}"), order);
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
            ctx.record_resolved_order(format!("timer:{step_id}"), order);
            if let Some(at_ms) = at_ms {
                ctx.record_resolved_at(format!("timer:{step_id}"), at_ms);
            }
            ctx.mark_timer_fired_at(step_id.clone(), Some(order as usize));
        }
        Some(resolution::What::Event(event)) => {
            ctx.buffer_journaled_event(
                event.name.clone(),
                event
                    .payload
                    .as_ref()
                    .map(|payload| payload.data.clone())
                    .unwrap_or_default(),
                Some(order as usize),
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
        Some(resolution::What::Cancel(_)) => ctx.note_cancel_requested(Some(order)),
    }
}

/// The category of a task step's failure, from the kernel protocol's code. A
/// failure with no code (or one this SDK does not know) takes it from how its
/// step ended, when that was a cancellation or a timeout, and is otherwise
/// uncategorized.
fn task_failure_code(code: i32, end: i32) -> crate::error::TaskFailureCode {
    use crate::error::TaskFailureCode;
    match FailureCode::try_from(code).map(TaskFailureCode::from) {
        Ok(code) if code != TaskFailureCode::Unspecified => code,
        _ => match End::try_from(end) {
            Ok(End::Cancelled) => TaskFailureCode::Cancelled,
            Ok(End::TimedOut) => TaskFailureCode::TimedOut,
            _ => TaskFailureCode::Unspecified,
        },
    }
}

/// The answer to an activation: what the workflow's code produced, as the
/// kernel protocol's commands, or its failure with the calls it made before
/// failing.
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
            ..Default::default()
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
            ..Default::default()
        })
        .collect();

    // The calls the code made before it failed, when the result says which
    // steps it reached: sdk-core holds them to history as it holds a
    // completing activation's. Unset, the failure is not checked.
    let mut calls_before_failure = None;
    let outcome = if !result.successful {
        if result.reached_steps.is_some() {
            calls_before_failure = commands(&result.commands, reached)
                .map(|commands| Commands {
                    commands,
                    ..Default::default()
                })
                .ok();
        }
        let error = result.error.clone().unwrap_or_else(|| {
            orcher_sdk_core::bridge::ExecutionError::workflow_error(
                "the workflow failed".to_string(),
                true,
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
            Ok(commands) => run_activation_result::Outcome::Commands(Commands {
                commands,
                ..Default::default()
            }),
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
        calls_before_failure,
        ..Default::default()
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
                ..Default::default()
            }),
        ),
        BridgeCommand::CancelTimer(cancel) => (
            cancel.sequence,
            String::new(),
            command::What::CancelTimer(kernel::CancelTimer {
                step_id: cancel.timer_id.clone(),
                ..Default::default()
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
                retry_policy: child.retry_policy.as_ref().map(workflow_retry_policy),
                ..Default::default()
            }),
        ),
        BridgeCommand::CancelChildWorkflow(cancel) => (
            0,
            String::new(),
            command::What::CancelChild(kernel::CancelChild {
                step_id: cancel.workflow_id.clone(),
                ..Default::default()
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
                ..Default::default()
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
                            ..Default::default()
                        }),
                    }),
                    attempts: record.execution_attempt.max(0) as u32,
                    ..Default::default()
                }),
                ..Default::default()
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
                ..Default::default()
            }),
        ),
        BridgeCommand::CompleteWorkflow(complete) => (
            0,
            String::new(),
            command::What::CompleteRun(kernel::CompleteRun {
                result: Some(payload(&complete.result)),
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            }),
        ),
        BridgeCommand::RestartFresh(restart) => (
            0,
            String::new(),
            command::What::RestartFresh(kernel::RestartFresh {
                workflow_type: restart.workflow_type.clone(),
                task_queue: restart.task_queue.clone().unwrap_or_default(),
                input: restart.input.iter().map(payload).collect(),
                execution_timeout: restart.timeout.map(duration),
                retry_policy: restart.retry_policy.as_ref().map(workflow_retry_policy),
                ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    }
}

fn retry_policy(policy: &TaskRetryPolicy) -> kernel::RetryPolicy {
    kernel::RetryPolicy {
        first_delay: Some(duration(policy.initial_interval)),
        delay_multiplier: policy.backoff_coefficient,
        max_delay: Some(duration(policy.max_interval)),
        max_attempts: policy.max_attempts,
        never_retry_types: policy.non_retryable_errors.clone(),
        ..Default::default()
    }
}

/// A child's or a fresh run's retry policy, its unset fields left unset for
/// the engine's workflow-retry defaults.
fn workflow_retry_policy(policy: &WorkflowRetryPolicy) -> kernel::RetryPolicy {
    kernel::RetryPolicy {
        first_delay: policy.initial_interval.map(duration),
        delay_multiplier: policy.backoff_coefficient,
        max_delay: policy.max_interval.map(duration),
        max_attempts: policy.max_attempts,
        never_retry_types: policy.non_retryable_errors.clone(),
        ..Default::default()
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

    fn failed_with(
        commands: Vec<BridgeCommand>,
        reached_steps: Option<&[&str]>,
    ) -> ExecutionResult {
        let mut result = ExecutionResult::failed(
            "run-1".into(),
            orcher_sdk_core::bridge::ExecutionError::workflow_error("out of stock".into(), false),
        );
        result.commands = commands;
        if let Some(steps) = reached_steps {
            result.set_reached_steps(steps.iter().map(|s| s.to_string()).collect());
        }
        result
    }

    /// A failure carries the calls made before it, answered from history or
    /// not, as a completing activation carries its calls.
    #[test]
    fn a_failure_carries_the_calls_made_before_it() {
        let answer = answer(
            vec![7],
            failed_with(vec![task(1, "ship_1")], Some(&["charge_0", "ship_1"])),
            vec![
                reached(StepKind::Task, "charge_0"),
                reached(StepKind::Task, "ship_1"),
            ],
        );
        assert!(matches!(
            answer.outcome,
            Some(run_activation_result::Outcome::WorkflowFailed(ref f)) if f.message == "out of stock"
        ));
        let calls: Vec<(bool, String)> = answer
            .calls_before_failure
            .expect("the calls before the failure")
            .commands
            .into_iter()
            .map(|c| (c.satisfied, c.step_id))
            .collect();
        assert_eq!(
            calls,
            [
                (true, "charge_0".to_string()),
                (false, "ship_1".to_string())
            ]
        );
    }

    /// Code that failed before any call says it made none; a failure that
    /// cannot say what calls were made (no steps reported) carries none.
    #[test]
    fn a_failure_says_when_it_made_no_call_and_carries_none_it_cannot_tell() {
        let none_made = answer(vec![7], failed_with(vec![], Some(&[])), vec![]);
        assert_eq!(none_made.calls_before_failure, Some(Commands::default()));
        let unsaid = answer(vec![7], failed_with(vec![], None), vec![]);
        assert_eq!(unsaid.calls_before_failure, None);
    }

    fn reached(kind: StepKind, id: &str) -> ReachedStep {
        ReachedStep {
            kind,
            id: id.into(),
        }
    }

    fn started_as(attempt: u32) -> RunActivation {
        RunActivation {
            run: Some(kernel::RunRef {
                workflow_id: "order-7".into(),
                run_id: "run-1".into(),
                workflow_type: "checkout".into(),
                task_queue: "orders".into(),
                namespace: "contract".into(),
                ..Default::default()
            }),
            resolutions: vec![kernel::Resolution {
                what: Some(resolution::What::Started(kernel::RunStarted {
                    attempt,
                    ..Default::default()
                })),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A run's attempt reads as the engine numbered it, from 1: the first
    /// run is attempt 1 and the run of its first retry attempt 2. An older
    /// engine's 0 for a first run reads as 1.
    #[test]
    fn a_runs_attempt_is_the_one_the_engine_numbered() {
        let first = load(started_as(1), "contract").unwrap().ctx;
        assert_eq!(first.attempt(), 1);
        assert!(first.execution().is_first_attempt());

        let retry = load(started_as(2), "contract").unwrap().ctx;
        assert_eq!(retry.attempt(), 2);
        assert!(!retry.execution().is_first_attempt());
        assert!(retry.execution().is_retry());

        // An engine before orcher#356 numbers the first run 0.
        let older_engine = load(started_as(0), "contract").unwrap().ctx;
        assert_eq!(older_engine.attempt(), 1);
        assert!(older_engine.execution().is_first_attempt());
    }

    /// A run whose task `work_0` failed and that was asked to cancel, both
    /// recorded at the same instant and listed in that order, numbered as
    /// given: the numbers, not the list, say which came first.
    fn failed_and_asked_to_cancel(failure: u64, cancel: u64) -> RunActivation {
        let at = Some(prost_types::Timestamp {
            seconds: 2,
            nanos: 0,
        });
        let mut activation = started_as(1);
        activation.resolutions[0].sequence = 1;
        activation.resolutions.extend([
            Resolution {
                step_id: "work_0".into(),
                at,
                sequence: failure,
                what: Some(resolution::What::Step(kernel::StepSettled {
                    outcome: Some(Outcome {
                        result: Some(outcome::Result::Failure(Failure {
                            message: "Task cancelled".into(),
                            error_type: "Cancelled".into(),
                            non_retryable: true,
                            code: FailureCode::Cancelled as i32,
                            details: vec![],
                            ..Default::default()
                        })),
                        end: End::Cancelled as i32,
                        attempts: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Resolution {
                at,
                sequence: cancel,
                what: Some(resolution::What::Cancel(kernel::CancelAsked {
                    reason: "user".into(),
                    requested_at: at,
                    ..Default::default()
                })),
                ..Default::default()
            },
        ]);
        activation
    }

    fn is_canceled<T>(result: &crate::Result<T>) -> bool {
        matches!(
            result,
            Err(crate::Error::Workflow(
                crate::error::WorkflowError::Canceled
            ))
        )
    }

    /// A task failure recorded in the cancel request's millisecond: the
    /// kernel's sequence says which came first. A failure before the request
    /// is the wait's result, and the request is delivered at the next wait; a
    /// failure after it is where the request is delivered, so cleanup runs.
    #[tokio::test]
    async fn a_failure_in_the_requests_millisecond_is_ordered_by_sequence() {
        let ctx = load(failed_and_asked_to_cancel(2, 3), "contract")
            .unwrap()
            .ctx;
        let failed: crate::Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert!(failed.is_err() && !is_canceled(&failed), "{failed:?}");
        let next: crate::Result<u32> = ctx.execute_task_by_name("work", 1).await;
        assert!(is_canceled(&next), "{next:?}");

        let ctx = load(failed_and_asked_to_cancel(3, 2), "contract")
            .unwrap()
            .ctx;
        let interrupted: crate::Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert!(is_canceled(&interrupted), "{interrupted:?}");
        let cleanup: crate::Result<u32> = ctx.execute_task_by_name("refund", 1).await;
        assert!(!is_canceled(&cleanup), "{cleanup:?}");
    }

    /// Resolutions with no sequence (built by hand) keep their order in the
    /// list: here the failure, then the request.
    #[tokio::test]
    async fn unnumbered_resolutions_keep_their_place_in_the_list() {
        let mut activation = failed_and_asked_to_cancel(3, 2);
        for resolution in &mut activation.resolutions {
            resolution.sequence = 0;
        }
        let ctx = load(activation, "contract").unwrap().ctx;
        let failed: crate::Result<u32> = ctx.execute_task_by_name("work", 0).await;
        assert!(failed.is_err() && !is_canceled(&failed), "{failed:?}");
        let next: crate::Result<u32> = ctx.execute_task_by_name("work", 1).await;
        assert!(is_canceled(&next), "{next:?}");
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
                        ..Default::default()
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

    /// An activation whose history holds one settled `charge` task step,
    /// failed with `failure` after `attempts` attempts and ending as `end`.
    fn failed_step(failure: Failure, end: End, attempts: u32) -> RunActivation {
        let mut activation = started_as(1);
        activation.resolutions.push(Resolution {
            step_id: "charge_0".into(),
            what: Some(resolution::What::Step(kernel::StepSettled {
                outcome: Some(Outcome {
                    result: Some(outcome::Result::Failure(failure)),
                    end: end as i32,
                    attempts,
                }),
            })),
            ..Default::default()
        });
        activation
    }

    /// What the workflow's code sees when it runs the failed `charge` step.
    async fn task_failure_seen(activation: RunActivation) -> crate::error::WorkflowError {
        let ctx = load(activation, "contract").unwrap().ctx;
        match ctx.execute_task::<_, _, String>("charge", 5u32).await {
            Err(crate::Error::Workflow(error)) => error,
            other => panic!("expected a workflow error, got {other:?}"),
        }
    }

    /// A cancelled task step reaches the workflow's code as `TaskFailed`
    /// carrying its type and the cancelled category, so the code can tell a
    /// cancellation from a failure of the task's own.
    #[tokio::test]
    async fn a_cancelled_task_step_carries_its_type_and_category() {
        let error = task_failure_seen(failed_step(
            Failure {
                message: "task was cancelled".into(),
                error_type: "Cancelled".into(),
                code: FailureCode::Cancelled as i32,
                ..Default::default()
            },
            End::Cancelled,
            1,
        ))
        .await;

        assert!(error.is_task_cancelled(), "{error:?}");
        assert_eq!(
            error.task_failure_code(),
            Some(crate::error::TaskFailureCode::Cancelled)
        );
        assert_eq!(error.task_failure_type(), Some("Cancelled"));
        match error {
            crate::error::WorkflowError::TaskFailed {
                task_type, reason, ..
            } => {
                assert_eq!(task_type, "charge");
                assert_eq!(reason, "task was cancelled");
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    /// A cancellation recorded only by its type, as sdk-core 0.15.0 records
    /// it (code `Application`), still reads as cancelled.
    #[tokio::test]
    async fn a_failure_of_type_cancelled_reads_as_cancelled() {
        let error = task_failure_seen(failed_step(
            Failure {
                message: "task was cancelled".into(),
                error_type: "Cancelled".into(),
                code: FailureCode::Application as i32,
                ..Default::default()
            },
            End::Failed,
            1,
        ))
        .await;

        assert!(error.is_task_cancelled(), "{error:?}");
        assert_eq!(
            error.task_failure_code(),
            Some(crate::error::TaskFailureCode::Application)
        );
        assert_eq!(error.task_failure_type(), Some("Cancelled"));
    }

    /// A task's own failure reaches the workflow's code with the type the
    /// task raised it under and the application category.
    #[tokio::test]
    async fn an_application_failure_carries_the_type_the_task_raised() {
        let error = task_failure_seen(failed_step(
            Failure {
                message: "card declined".into(),
                error_type: "CardDeclined".into(),
                code: FailureCode::Application as i32,
                ..Default::default()
            },
            End::Failed,
            3,
        ))
        .await;

        assert!(!error.is_task_cancelled(), "{error:?}");
        match error {
            crate::error::WorkflowError::TaskFailed {
                attempts,
                reason,
                error_type,
                code,
                ..
            } => {
                assert_eq!(attempts, 3);
                assert_eq!(reason, "card declined");
                assert_eq!(error_type, "CardDeclined");
                assert_eq!(code, crate::error::TaskFailureCode::Application);
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    /// Every kernel failure code maps to its own category, and a code this
    /// SDK does not know is uncategorized rather than refused.
    #[test]
    fn every_failure_code_maps_to_its_category() {
        use crate::error::TaskFailureCode as Code;
        let cases = [
            (FailureCode::Unspecified, Code::Unspecified),
            (FailureCode::Application, Code::Application),
            (FailureCode::TimedOut, Code::TimedOut),
            (FailureCode::Cancelled, Code::Cancelled),
            (FailureCode::Terminated, Code::Terminated),
            (FailureCode::NonDeterminism, Code::NonDeterminism),
            (FailureCode::PayloadTooLarge, Code::PayloadTooLarge),
            (FailureCode::InvalidCommand, Code::InvalidCommand),
        ];
        for (wire, code) in cases {
            assert_eq!(
                task_failure_code(wire as i32, End::Failed as i32),
                code,
                "{wire:?}"
            );
            // The category survives the journaled sentinel's round trip.
            assert_eq!(Code::from_name(code.as_str()), code, "{code}");
        }
        assert_eq!(
            task_failure_code(9_999, End::Failed as i32),
            Code::Unspecified
        );
    }

    /// A failure with no code takes a cancellation or a timeout from how its
    /// step ended; a code, when there is one, wins.
    #[test]
    fn a_failure_with_no_code_takes_it_from_how_the_step_ended() {
        use crate::error::TaskFailureCode as Code;
        let unspecified = FailureCode::Unspecified as i32;
        assert_eq!(
            task_failure_code(unspecified, End::Cancelled as i32),
            Code::Cancelled
        );
        assert_eq!(
            task_failure_code(unspecified, End::TimedOut as i32),
            Code::TimedOut
        );
        assert_eq!(
            task_failure_code(unspecified, End::Failed as i32),
            Code::Unspecified
        );
        assert_eq!(
            task_failure_code(9_999, End::Cancelled as i32),
            Code::Cancelled
        );
        assert_eq!(
            task_failure_code(FailureCode::Application as i32, End::Cancelled as i32),
            Code::Application
        );
    }

    /// A restart keeps the execution timeout the workflow asked for.
    #[test]
    fn a_restart_carries_its_execution_timeout() {
        let issued = vec![BridgeCommand::RestartFresh(
            orcher_sdk_core::bridge::RestartFreshCommand {
                workflow_type: "checkout".into(),
                input: vec![],
                task_queue: None,
                timeout: Some(Duration::from_secs(3600)),
                ..Default::default()
            },
        )];
        let calls = commands(&issued, vec![]).unwrap();
        let Some(command::What::RestartFresh(restart)) = &calls[0].what else {
            panic!("{calls:?}");
        };
        assert_eq!(
            restart.execution_timeout,
            Some(duration(Duration::from_secs(3600)))
        );
    }

    /// The kernel command a workflow's child start or restart-fresh becomes,
    /// issued through the user-facing options.
    async fn issued_through_options(
        child: crate::workflow::ChildWorkflowOptions,
        restart: crate::workflow::RestartFreshOptions,
    ) -> (Option<kernel::RetryPolicy>, Option<kernel::RetryPolicy>) {
        use crate::worker::runtime::{ExecutionRuntime, RuntimeConfig};
        let ctx = WorkflowContext::new(
            WorkflowExecution {
                workflow_id: "order-7".into(),
                run_id: "run-1".into(),
                workflow_type: "checkout".into(),
                attempt: 1,
                namespace: "contract".into(),
                task_queue: "orders".into(),
            },
            false,
            "1.0".into(),
        );
        ctx.start_child_workflow_with_options("ship", 7, child)
            .await
            .unwrap();
        ctx.restart_fresh_with(8, Some(restart)).unwrap();
        let runtime = ExecutionRuntime::new(RuntimeConfig::default());
        let mut policies = ctx.take_commands().into_iter().map(|issued| {
            let bridge = runtime.convert_command(issued, "orders").unwrap();
            match kernel_command(&bridge).unwrap().what {
                Some(command::What::StartChild(child)) => child.retry_policy,
                Some(command::What::RestartFresh(restart)) => restart.retry_policy,
                other => panic!("{other:?}"),
            }
        });
        let child = policies.next().unwrap();
        let restart = policies.next().unwrap();
        assert!(policies.next().is_none());
        (child, restart)
    }

    /// A child's and a restart-fresh's retry policy set on their options
    /// reaches the kernel commands as a workflow start sends one: delays and
    /// errors as given, and the count as given.
    #[tokio::test]
    async fn a_child_and_restart_fresh_retry_policy_reach_the_kernel() {
        use crate::task::RetryPolicy;
        let policy = RetryPolicy::default()
            .with_max_attempts(2)
            .with_initial_interval(Duration::from_millis(500))
            .with_max_interval(Duration::from_secs(9))
            .with_backoff_coefficient(1.5)
            .with_non_retryable_errors(["Declined"]);
        let expected = kernel::RetryPolicy {
            max_attempts: 2,
            first_delay: Some(duration(Duration::from_millis(500))),
            max_delay: Some(duration(Duration::from_secs(9))),
            delay_multiplier: 1.5,
            never_retry_types: vec!["Declined".into()],
            ..Default::default()
        };
        let (child, restart) = issued_through_options(
            crate::workflow::ChildWorkflowOptions::new().retry_policy(policy.clone()),
            crate::workflow::RestartFreshOptions::new().retry_policy(policy),
        )
        .await;
        assert_eq!(child, Some(expected.clone()));
        assert_eq!(restart, Some(expected));
    }

    /// A policy that leaves the count alone sends the default of 3; a count
    /// of 0 sends 0, retry without limit.
    #[tokio::test]
    async fn a_child_and_restart_fresh_retry_count_is_sent_as_a_workflow_start_sends_it() {
        use crate::task::RetryPolicy;
        let default_count = RetryPolicy::default().with_initial_interval(Duration::from_secs(2));
        let (child, restart) = issued_through_options(
            crate::workflow::ChildWorkflowOptions::new().retry_policy(default_count.clone()),
            crate::workflow::RestartFreshOptions::new().retry_policy(default_count),
        )
        .await;
        assert_eq!(child.unwrap().max_attempts, 3);
        assert_eq!(restart.unwrap().max_attempts, 3);

        let unlimited = RetryPolicy::default().with_max_attempts(0);
        let (child, restart) = issued_through_options(
            crate::workflow::ChildWorkflowOptions::new().retry_policy(unlimited.clone()),
            crate::workflow::RestartFreshOptions::new().retry_policy(unlimited),
        )
        .await;
        assert_eq!(child.unwrap().max_attempts, 0);
        assert_eq!(restart.unwrap().max_attempts, 0);
    }

    /// Without a retry policy on the options, none is sent: the child is not
    /// retried and the restarted-fresh run keeps its current policy.
    #[tokio::test]
    async fn a_child_and_restart_fresh_without_a_retry_policy_send_none() {
        let (child, restart) = issued_through_options(
            crate::workflow::ChildWorkflowOptions::new(),
            crate::workflow::RestartFreshOptions::new(),
        )
        .await;
        assert_eq!(child, None);
        assert_eq!(restart, None);
    }

    /// A child's and a restart's retry policies reach the kernel commands,
    /// unset delays unset; without one, none is sent.
    #[test]
    fn child_and_restart_retry_policies_reach_the_kernel() {
        let policy = WorkflowRetryPolicy {
            max_attempts: 3,
            initial_interval: Some(Duration::from_secs(5)),
            max_interval: None,
            backoff_coefficient: 1.5,
            non_retryable_errors: vec!["Declined".into()],
        };
        let expected = kernel::RetryPolicy {
            max_attempts: 3,
            first_delay: Some(duration(Duration::from_secs(5))),
            max_delay: None,
            delay_multiplier: 1.5,
            never_retry_types: vec!["Declined".into()],
            ..Default::default()
        };
        let child = |retry_policy| {
            BridgeCommand::StartChildWorkflow(orcher_sdk_core::bridge::StartChildWorkflowCommand {
                sequence: 1,
                workflow_id: "order-7-child-1".into(),
                workflow_type: "ship".into(),
                retry_policy,
                ..Default::default()
            })
        };
        let restart = |retry_policy| {
            BridgeCommand::RestartFresh(orcher_sdk_core::bridge::RestartFreshCommand {
                retry_policy,
                ..Default::default()
            })
        };
        let child_policy = |command: &BridgeCommand| match kernel_command(command).unwrap().what {
            Some(command::What::StartChild(child)) => child.retry_policy,
            other => panic!("{other:?}"),
        };
        let restart_policy = |command: &BridgeCommand| match kernel_command(command).unwrap().what {
            Some(command::What::RestartFresh(restart)) => restart.retry_policy,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            child_policy(&child(Some(policy.clone()))),
            Some(expected.clone())
        );
        assert_eq!(child_policy(&child(None)), None);
        assert_eq!(restart_policy(&restart(Some(policy))), Some(expected));
        assert_eq!(restart_policy(&restart(None)), None);
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
