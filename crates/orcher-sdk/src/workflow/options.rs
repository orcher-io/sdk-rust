//! Options for tasks, child workflows and fresh restarts.

use std::time::Duration;

/// What happens to a child workflow when its parent completes, fails or is canceled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OrphanPolicy {
    /// Terminate the child immediately, without giving it a chance to clean up.
    Terminate,

    /// Leave the child running as an independent workflow.
    Abandon,

    /// Request cancellation of the child, which lets it clean up. The default.
    #[default]
    Cancel,
}

// `RetryPolicy` lives in the task module; re-exported here for convenience.
pub use crate::task::RetryPolicy;

/// Options for running a task from a workflow: timeouts, retries and task ID.
///
/// # Example
///
/// ```rust
/// # use orcher_sdk::workflow::TaskOptions;
/// # use orcher_sdk::task::RetryPolicy;
/// # use std::time::Duration;
/// let options = TaskOptions::default()
///     .with_timeout(Duration::from_secs(300))
///     .with_task_id("payment-task")
///     .with_retry_policy(RetryPolicy::default())
///     .with_heartbeat_timeout(Duration::from_secs(30));
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TaskOptions {
    /// Maximum time for one run of the task.
    ///
    /// A run that exceeds it is canceled and retried according to the retry
    /// policy. Time spent waiting in the queue does not count; see
    /// `queue_timeout`.
    pub timeout: Option<Duration>,

    /// Stable task ID. If not set, one is generated.
    pub task_id: Option<String>,

    /// How the task is retried on failure.
    pub retry_policy: Option<RetryPolicy>,

    /// Maximum time between heartbeats before the task is considered failed.
    pub heartbeat_timeout: Option<Duration>,

    /// How long the task may wait in its queue before a worker starts it.
    ///
    /// `None`, the default, means it waits as long as needed. This is separate
    /// from `timeout`, which covers only the run itself.
    pub queue_timeout: Option<Duration>,
}

impl TaskOptions {
    /// Sets the maximum time for one run of the task.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets a stable task ID.
    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    /// Sets the retry policy.
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Sets the heartbeat timeout.
    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// Sets how long the task may wait in its queue before a worker starts it.
    pub fn with_queue_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = Some(timeout);
        self
    }
}

/// Options for starting a child workflow.
///
/// # Example
///
/// ```rust
/// # use orcher_sdk::workflow::{ChildWorkflowOptions, OrphanPolicy};
/// # use std::time::Duration;
/// let options = ChildWorkflowOptions::new()
///     .workflow_id("child-order-123")
///     .execution_timeout(Duration::from_secs(3600))
///     .task_queue("child-queue")
///     .orphan_policy(OrphanPolicy::Cancel);
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ChildWorkflowOptions {
    /// Child workflow ID. If not set, a unique one is generated.
    pub workflow_id: Option<String>,

    /// Maximum time for the child workflow to complete.
    pub execution_timeout: Option<Duration>,

    /// Task queue for the child. If not set, the parent's task queue is used.
    pub task_queue: Option<String>,

    /// What happens to the child when the parent closes.
    pub orphan_policy: OrphanPolicy,
}

impl ChildWorkflowOptions {
    /// Creates options with every field unset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the child workflow ID.
    pub fn workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = Some(workflow_id.into());
        self
    }

    /// Sets the child's execution timeout.
    pub fn execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = Some(timeout);
        self
    }

    /// Sets the child's task queue.
    pub fn task_queue(mut self, queue: impl Into<String>) -> Self {
        self.task_queue = Some(queue.into());
        self
    }

    /// Sets what happens to the child when the parent closes.
    pub fn orphan_policy(mut self, policy: OrphanPolicy) -> Self {
        self.orphan_policy = policy;
        self
    }
}

/// Options for restarting a workflow as a fresh execution.
///
/// Restarting fresh starts a new execution with the same workflow ID and an
/// empty history, carrying state forward as input. Long-running workflows,
/// such as subscriptions or periodic jobs, use it to keep their history from
/// growing without bound and to pick up new code.
///
/// # Example
///
/// ```rust
/// # use orcher_sdk::workflow::RestartFreshOptions;
/// # use std::time::Duration;
/// let options = RestartFreshOptions::new()
///     .workflow_type("UpdatedWorkflow")
///     .task_queue("updated-queue")
///     .timeout(Duration::from_secs(3600));
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RestartFreshOptions {
    /// Workflow type for the new execution. If not set, the type is unchanged.
    ///
    /// Useful when moving to a new workflow implementation.
    pub workflow_type: Option<String>,

    /// Task queue for the new execution. If not set, the queue is unchanged.
    pub task_queue: Option<String>,

    /// Execution timeout for the new execution. If not set, it is unchanged.
    pub timeout: Option<Duration>,
}

impl RestartFreshOptions {
    /// Creates options that keep the workflow type, task queue and timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the workflow type for the new execution.
    pub fn workflow_type(mut self, workflow_type: impl Into<String>) -> Self {
        self.workflow_type = Some(workflow_type.into());
        self
    }

    /// Sets the task queue for the new execution.
    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(task_queue.into());
        self
    }

    /// Sets the execution timeout for the new execution.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_options_defaults() {
        let options = TaskOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.task_id.is_none());
        assert!(options.retry_policy.is_none());
        assert!(options.heartbeat_timeout.is_none());
    }

    #[test]
    fn test_child_workflow_options_defaults() {
        let options = ChildWorkflowOptions::default();
        assert!(options.workflow_id.is_none());
        assert!(options.execution_timeout.is_none());
        assert!(options.task_queue.is_none());
        assert_eq!(options.orphan_policy, OrphanPolicy::Cancel);
    }

    #[test]
    fn test_orphan_policy_default() {
        assert_eq!(OrphanPolicy::default(), OrphanPolicy::Cancel);
    }

    #[test]
    fn test_orphan_policy_variants() {
        let terminate = OrphanPolicy::Terminate;
        let abandon = OrphanPolicy::Abandon;
        let request_cancel = OrphanPolicy::Cancel;

        assert_ne!(terminate, abandon);
        assert_ne!(terminate, request_cancel);
        assert_ne!(abandon, request_cancel);
    }

    #[test]
    fn test_restart_fresh_options_defaults() {
        let options = RestartFreshOptions::default();
        assert!(options.workflow_type.is_none());
        assert!(options.task_queue.is_none());
        assert!(options.timeout.is_none());
    }

    #[test]
    fn test_restart_fresh_options_builder() {
        let options = RestartFreshOptions::new()
            .workflow_type("NewWorkflow")
            .task_queue("new-queue")
            .timeout(Duration::from_secs(600));

        assert_eq!(options.workflow_type.as_deref(), Some("NewWorkflow"));
        assert_eq!(options.task_queue.as_deref(), Some("new-queue"));
        assert_eq!(options.timeout, Some(Duration::from_secs(600)));
    }
}
