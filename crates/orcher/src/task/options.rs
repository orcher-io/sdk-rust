//! Options for how a task is executed, and information about an attempt.

use std::time::Duration;

/// Timeouts and retry settings for executing a task.
///
/// # Example
///
/// ```rust
/// # use orcher::task::TaskExecutionOptions;
/// # use std::time::Duration;
/// let options = TaskExecutionOptions::default()
///     .with_timeout(Duration::from_secs(300))
///     .with_heartbeat_timeout(Duration::from_secs(30))
///     .with_max_attempts(3)
///     .with_retry_initial_interval(Duration::from_secs(1));
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TaskExecutionOptions {
    /// Maximum execution time.
    pub timeout: Option<Duration>,

    /// How long the task may go without a heartbeat before it is considered lost.
    pub heartbeat_timeout: Option<Duration>,

    /// Maximum number of attempts, including the first.
    pub max_attempts: Option<u32>,

    /// Delay before the first retry.
    pub retry_initial_interval: Option<Duration>,

    /// Upper bound on the delay between retries.
    pub retry_max_interval: Option<Duration>,

    /// Factor the retry delay is multiplied by after each attempt.
    pub retry_backoff_coefficient: Option<f64>,
}

impl TaskExecutionOptions {
    /// Set the maximum execution time.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the heartbeat timeout.
    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of attempts.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Set the initial retry interval.
    pub fn with_retry_initial_interval(mut self, interval: Duration) -> Self {
        self.retry_initial_interval = Some(interval);
        self
    }

    /// Set the maximum retry interval.
    pub fn with_retry_max_interval(mut self, interval: Duration) -> Self {
        self.retry_max_interval = Some(interval);
        self
    }

    /// Set the retry backoff coefficient.
    pub fn with_retry_backoff_coefficient(mut self, coefficient: f64) -> Self {
        self.retry_backoff_coefficient = Some(coefficient);
        self
    }
}

/// Information about one attempt at executing a task.
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// The task type name.
    pub task_type: String,

    /// The ID of the workflow that scheduled the task.
    pub workflow_id: String,

    /// The workflow run ID.
    pub run_id: String,

    /// The task ID.
    pub task_id: String,

    /// The attempt number, starting at 1.
    pub attempt: i32,

    /// The task queue the task was scheduled on.
    pub task_queue: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_execution_options_defaults() {
        let options = TaskExecutionOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.heartbeat_timeout.is_none());
        assert!(options.max_attempts.is_none());
        assert!(options.retry_initial_interval.is_none());
        assert!(options.retry_max_interval.is_none());
        assert!(options.retry_backoff_coefficient.is_none());
    }

    #[test]
    fn test_task_info() {
        let info = TaskInfo {
            task_type: "ProcessPayment".to_string(),
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            task_id: "task-789".to_string(),
            attempt: 1,
            task_queue: "payments".to_string(),
        };

        assert_eq!(info.task_type, "ProcessPayment");
        assert_eq!(info.workflow_id, "wf-123");
        assert_eq!(info.task_id, "task-789");
    }
}
