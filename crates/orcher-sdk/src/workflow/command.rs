//! Commands a workflow run produces for the engine.
//!
//! `WorkflowContext` queues these as the workflow schedules tasks, starts
//! timers and so on. The worker converts them to sdk-core bridge commands when
//! the run completes or suspends.
//!
//! These types are public for testing support. Workflow code should use the
//! `WorkflowContext` methods instead.

use crate::workflow::TaskOptions;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A decision made by a workflow run, to be sent to the engine.
///
/// Public for testing support; workflow code should use the
/// `WorkflowContext` methods instead.
#[derive(Debug, Clone)]
pub enum WorkflowCommand {
    /// Schedule a task on a worker.
    ScheduleTask(ScheduleTaskCommand),

    /// Start a timer.
    StartTimer(StartTimerCommand),

    /// Complete the workflow successfully.
    CompleteWorkflow(CompleteWorkflowCommand),

    /// Fail the workflow.
    FailWorkflow(FailWorkflowCommand),

    /// Start a child workflow.
    StartChildWorkflow(StartChildWorkflowCommand),

    /// Cancel a timer.
    CancelTimer(CancelTimerCommand),

    /// Send an event to another workflow.
    SendEvent(SendEventCommand),

    /// Cancel a child workflow this workflow started.
    CancelChildWorkflow(CancelChildWorkflowCommand),

    /// Wait for an event.
    WaitForEvent(WaitForEventCommand),

    /// Restart the workflow as a fresh execution.
    RestartFresh(RestartFreshCommand),

    /// Record the result of an inline closure so it can be replayed.
    RecordStepResult(RecordStepResultCommand),
}

/// Schedules a task on a worker.
#[derive(Debug, Clone)]
pub struct ScheduleTaskCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// ID that matches the task's journaled result to this call on replay.
    pub task_id: String,

    /// Registered task name.
    pub task_type: String,

    /// Queue the task is dispatched on.
    pub task_queue: String,

    /// JSON-encoded input.
    pub input: Vec<u8>,

    /// Maximum time for one run of the task, not counting time in the queue.
    pub timeout: Duration,

    /// Maximum time between heartbeats before the task is considered failed.
    ///
    /// `None` leaves heartbeat supervision off for this task.
    pub heartbeat_timeout: Option<Duration>,

    /// How long the task may wait in its queue before a worker starts it.
    /// `None` means no queue limit.
    pub queue_timeout: Option<Duration>,

    /// Retry policy for the task.
    pub retry_policy: Option<TaskRetryPolicy>,

    /// Custom headers passed to the task.
    pub headers: Vec<(String, Vec<u8>)>,
}

/// Retry policy sent with a scheduled task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetryPolicy {
    /// Maximum number of attempts.
    pub max_attempts: u32,

    /// Delay before the first retry.
    pub initial_interval: Duration,

    /// Upper bound on the delay between retries.
    pub max_interval: Duration,

    /// Factor the delay is multiplied by after each retry.
    pub backoff_coefficient: f64,

    /// Error types that are never retried.
    pub non_retryable_errors: Vec<String>,
}

/// Records the outcome of an inline closure so the engine can journal it.
///
/// Closures passed to `ctx.execute("step", closure)` run in the workflow
/// process and keep their results in `pending_task_results`. When the run
/// completes, the worker turns them into these commands with
/// [`extract_closure_commands`](crate::workflow::closure_commands::extract_closure_commands).
/// Once journaled, a closure's result is replayed instead of the closure
/// running again, and closures show up in the execution history next to tasks.
#[derive(Debug, Clone)]
pub struct RecordStepResultCommand {
    /// Step ID: the step name plus its sequence number, e.g. "fetch_user_1".
    pub step_name: String,

    /// A `StepType` value (task, closure, child workflow, side effect).
    pub step_type: i32,

    /// JSON-encoded result; empty when the step failed.
    pub result: Vec<u8>,

    /// Failure details, set when the step failed.
    pub failure: Option<orcher_proto::orcher::v1::Failure>,

    /// How many times the step has run, including this attempt.
    pub execution_attempt: i32,
}

/// Starts a durable timer.
#[derive(Debug, Clone)]
pub struct StartTimerCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// ID that identifies the timer when it fires or is canceled.
    pub timer_id: String,

    /// How long the timer runs.
    pub duration: Duration,
}

/// Completes the workflow successfully.
#[derive(Debug, Clone)]
pub struct CompleteWorkflowCommand {
    /// JSON-encoded workflow result.
    pub result: Vec<u8>,
}

/// Fails the workflow.
#[derive(Debug, Clone)]
pub struct FailWorkflowCommand {
    /// Error message.
    pub message: String,

    /// Serialized error details.
    pub details: Option<Vec<u8>>,

    /// Stack trace, if one was captured.
    pub stack_trace: Option<String>,
}

/// Starts a child workflow.
#[derive(Debug, Clone)]
pub struct StartChildWorkflowCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// Child workflow ID.
    pub workflow_id: String,

    /// Child workflow type.
    pub workflow_type: String,

    /// Task queue the child runs on.
    pub task_queue: String,

    /// JSON-encoded child input.
    pub input: Vec<u8>,

    /// Execution timeout for the child.
    pub timeout: Option<Duration>,

    /// What happens to the child when the parent closes.
    pub orphan_policy: OrphanPolicy,
}

/// What happens to a child workflow when its parent closes.
///
/// Wire form of [`crate::workflow::OrphanPolicy`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OrphanPolicy {
    /// Terminate the child immediately.
    Terminate,
    /// Leave the child running.
    Abandon,
    /// Request cancellation, letting the child clean up.
    Cancel,
}

/// Cancels a timer.
#[derive(Debug, Clone)]
pub struct CancelTimerCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// ID of the timer to cancel.
    pub timer_id: String,
}

/// Sends an event to another workflow.
#[derive(Debug, Clone)]
pub struct SendEventCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// ID of the workflow that receives the event.
    pub workflow_id: String,

    /// Event name.
    pub event_name: String,

    /// Serialized event payload.
    pub data: Vec<u8>,
}

/// Cancels a child workflow this workflow started.
#[derive(Debug, Clone)]
pub struct CancelChildWorkflowCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// ID the parent gave the child.
    pub workflow_id: String,
}

/// Waits for an event sent to this workflow.
#[derive(Debug, Clone)]
pub struct WaitForEventCommand {
    /// Position of the command in the workflow; identical on every replay.
    pub sequence: u64,

    /// Name of the event to wait for.
    pub event_name: String,

    /// How long to wait; `None` waits indefinitely.
    pub timeout: Option<Duration>,
}

/// Restarts the workflow as a fresh execution with an empty history.
#[derive(Debug, Clone)]
pub struct RestartFreshCommand {
    /// Workflow type for the new execution; `None` keeps the current one.
    pub workflow_type: Option<String>,

    /// JSON-encoded input for the new execution.
    pub input: Vec<u8>,

    /// Task queue for the new execution; `None` keeps the current one.
    pub task_queue: Option<String>,

    /// Execution timeout for the new execution; `None` keeps the current one.
    pub timeout: Option<Duration>,
}

impl WorkflowCommand {
    /// Returns `true` if the command ends the current execution.
    ///
    /// Complete, fail and restart-fresh are terminal. A run may emit at most one.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowCommand::CompleteWorkflow(_)
                | WorkflowCommand::FailWorkflow(_)
                | WorkflowCommand::RestartFresh(_)
        )
    }

    /// Returns the command's variant name, for logging.
    pub fn command_type(&self) -> &'static str {
        match self {
            WorkflowCommand::ScheduleTask(_) => "ScheduleTask",
            WorkflowCommand::StartTimer(_) => "StartTimer",
            WorkflowCommand::CompleteWorkflow(_) => "CompleteWorkflow",
            WorkflowCommand::FailWorkflow(_) => "FailWorkflow",
            WorkflowCommand::StartChildWorkflow(_) => "StartChildWorkflow",
            WorkflowCommand::CancelTimer(_) => "CancelTimer",
            WorkflowCommand::SendEvent(_) => "SendEvent",
            WorkflowCommand::CancelChildWorkflow(_) => "CancelChildWorkflow",
            WorkflowCommand::WaitForEvent(_) => "WaitForEvent",
            WorkflowCommand::RestartFresh(_) => "RestartFresh",
            WorkflowCommand::RecordStepResult(_) => "RecordStepResult",
        }
    }
}

impl From<&TaskOptions> for Option<TaskRetryPolicy> {
    fn from(options: &TaskOptions) -> Self {
        options.retry_policy.as_ref().map(|policy| TaskRetryPolicy {
            max_attempts: policy.max_attempts,
            initial_interval: policy.initial_interval,
            max_interval: policy.max_interval,
            backoff_coefficient: policy.backoff_coefficient,
            non_retryable_errors: policy.non_retryable_errors.clone(),
        })
    }
}

impl From<crate::workflow::options::OrphanPolicy> for OrphanPolicy {
    fn from(policy: crate::workflow::options::OrphanPolicy) -> Self {
        match policy {
            crate::workflow::options::OrphanPolicy::Terminate => OrphanPolicy::Terminate,
            crate::workflow::options::OrphanPolicy::Abandon => OrphanPolicy::Abandon,
            crate::workflow::options::OrphanPolicy::Cancel => OrphanPolicy::Cancel,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_is_terminal() {
        let complete =
            WorkflowCommand::CompleteWorkflow(CompleteWorkflowCommand { result: vec![] });
        assert!(complete.is_terminal());

        let fail = WorkflowCommand::FailWorkflow(FailWorkflowCommand {
            message: "error".to_string(),
            details: None,
            stack_trace: None,
        });
        assert!(fail.is_terminal());

        let timer = WorkflowCommand::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        });
        assert!(!timer.is_terminal());
    }

    #[test]
    fn test_command_type() {
        let task = WorkflowCommand::ScheduleTask(ScheduleTaskCommand {
            sequence: 1,
            task_id: "task-1".to_string(),
            task_type: "MyTask".to_string(),
            task_queue: "default".to_string(),
            input: vec![],
            timeout: Duration::from_secs(300),
            heartbeat_timeout: None,
            queue_timeout: None,
            retry_policy: None,
            headers: vec![],
        });
        assert_eq!(task.command_type(), "ScheduleTask");
    }

    #[test]
    fn test_retry_policy_from_task_options() {
        use crate::workflow::{RetryPolicy, TaskOptions};

        let options = TaskOptions {
            retry_policy: Some(RetryPolicy {
                max_attempts: 5,
                initial_interval: Duration::from_secs(1),
                max_interval: Duration::from_secs(30),
                backoff_coefficient: 2.0,
                jitter: crate::task::JitterStrategy::None,
                non_retryable_errors: vec![],
            }),
            ..Default::default()
        };

        let retry: Option<TaskRetryPolicy> = (&options).into();
        assert!(retry.is_some());

        let retry = retry.unwrap();
        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.backoff_coefficient, 2.0);
    }
}
