//! Identifiers and placement of a workflow execution.

use serde::{Deserialize, Serialize};

/// Identifiers, namespace and task queue of a workflow execution.
///
/// # Example
///
/// ```rust
/// # use orcher_sdk::workflow::WorkflowExecution;
/// let execution = WorkflowExecution {
///     workflow_id: "order-123".to_string(),
///     run_id: "run-456".to_string(),
///     workflow_type: "OrderWorkflow".to_string(),
///     attempt: 1,
///     namespace: "default".to_string(),
///     task_queue: "orders".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    /// Unique identifier of the workflow instance.
    pub workflow_id: String,

    /// Unique identifier of this run.
    ///
    /// One workflow can have several runs, for example when it is retried or restarted fresh.
    pub run_id: String,

    /// Workflow type name, such as `"OrderWorkflow"`.
    pub workflow_type: String,

    /// Attempt number, starting at 1 and incremented on each retry after a failure.
    pub attempt: i32,

    /// Namespace the workflow belongs to.
    ///
    /// Namespaces isolate environments or tenants, such as `"production"` or `"tenant-123"`.
    pub namespace: String,

    /// Task queue the workflow runs on. Only workers polling this queue execute it.
    pub task_queue: String,
}

impl WorkflowExecution {
    /// Creates execution metadata for the first attempt (`attempt` is 1).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::workflow::WorkflowExecution;
    /// let execution = WorkflowExecution::new(
    ///     "order-123".to_string(),
    ///     "run-456".to_string(),
    ///     "OrderWorkflow".to_string(),
    ///     "default".to_string(),
    ///     "orders".to_string(),
    /// );
    /// ```
    pub fn new(
        workflow_id: String,
        run_id: String,
        workflow_type: String,
        namespace: String,
        task_queue: String,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            workflow_type,
            attempt: 1,
            namespace,
            task_queue,
        }
    }

    /// Sets the attempt number, typically when rebuilding metadata for replay.
    pub fn with_attempt(mut self, attempt: i32) -> Self {
        self.attempt = attempt;
        self
    }

    /// Returns `true` on the first attempt.
    pub fn is_first_attempt(&self) -> bool {
        self.attempt == 1
    }

    /// Returns `true` on any attempt after the first.
    pub fn is_retry(&self) -> bool {
        self.attempt > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_execution_creation() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        assert_eq!(execution.workflow_id, "wf-123");
        assert_eq!(execution.run_id, "run-456");
        assert_eq!(execution.workflow_type, "TestWorkflow");
        assert_eq!(execution.attempt, 1);
        assert!(execution.is_first_attempt());
        assert!(!execution.is_retry());
    }

    #[test]
    fn test_workflow_execution_new() {
        let execution = WorkflowExecution::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "TestWorkflow".to_string(),
            "production".to_string(),
            "critical".to_string(),
        );

        assert_eq!(execution.workflow_id, "wf-123");
        assert_eq!(execution.run_id, "run-456");
        assert_eq!(execution.workflow_type, "TestWorkflow");
        assert_eq!(execution.namespace, "production");
        assert_eq!(execution.task_queue, "critical");
        assert_eq!(execution.attempt, 1);
    }

    #[test]
    fn test_workflow_execution_with_attempt() {
        let execution = WorkflowExecution::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "TestWorkflow".to_string(),
            "default".to_string(),
            "test".to_string(),
        )
        .with_attempt(3);

        assert_eq!(execution.attempt, 3);
        assert!(!execution.is_first_attempt());
        assert!(execution.is_retry());
    }

    #[test]
    fn test_serialization() {
        let execution = WorkflowExecution::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "TestWorkflow".to_string(),
            "default".to_string(),
            "test".to_string(),
        );

        let json = serde_json::to_string(&execution).unwrap();
        assert!(json.contains("wf-123"));
        assert!(json.contains("run-456"));

        let deserialized: WorkflowExecution = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.workflow_id, execution.workflow_id);
        assert_eq!(deserialized.run_id, execution.run_id);
    }
}
