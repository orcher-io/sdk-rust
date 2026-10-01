//! Assertion methods on `ExecutionTrace`, for checking a single recorded run.
//!
//! They complement the [`TestEnv`](crate::testing::TestEnv) assertions, which look a
//! run up by workflow ID.
//!
//! # Example
//!
//! ```rust
//! # use orcher_sdk::testing::ExecutionTrace;
//! # fn example(trace: &ExecutionTrace) {
//! trace.assert_completed();
//! trace.assert_task_scheduled("process_payment");
//! trace.assert_task_count(3);
//! # }
//! ```

use crate::testing::env::{ExecutionStatus, ExecutionTrace};

#[allow(dead_code)] // Called by users' tests, not by the crate
impl ExecutionTrace {
    /// Assert that the workflow completed successfully.
    ///
    /// # Panics
    ///
    /// Panics if the workflow status is not `Completed`.
    pub fn assert_completed(&self) {
        assert_eq!(
            self.status,
            ExecutionStatus::Completed,
            "Expected workflow '{}' to be completed, but status was {:?}",
            self.workflow_type,
            self.status
        );
    }

    /// Assert that the workflow failed.
    ///
    /// # Panics
    ///
    /// Panics if the workflow status is not `Failed`.
    pub fn assert_failed(&self) {
        assert!(
            matches!(self.status, ExecutionStatus::Failed(_)),
            "Expected workflow '{}' to have failed, but status was {:?}",
            self.workflow_type,
            self.status
        );
    }

    /// Assert that the workflow failed with a message containing `message_substring`.
    ///
    /// # Panics
    ///
    /// Panics if the workflow did not fail or the message does not contain the substring.
    pub fn assert_failed_with(&self, message_substring: &str) {
        match &self.status {
            ExecutionStatus::Failed(msg) => {
                assert!(
                    msg.contains(message_substring),
                    "Expected failure message to contain '{}', but got: {}",
                    message_substring,
                    msg
                );
            }
            other => {
                panic!(
                    "Expected workflow '{}' to have failed, but status was {:?}",
                    self.workflow_type, other
                );
            }
        }
    }

    /// Assert that a task with the given name was executed.
    ///
    /// # Panics
    ///
    /// Panics if no task with the given name was executed.
    pub fn assert_task_scheduled(&self, task_name: &str) {
        assert!(
            self.tasks_executed.contains(&task_name.to_string()),
            "Expected task '{}' to be scheduled, but it was not. Executed tasks: {:?}",
            task_name,
            self.tasks_executed
        );
    }

    /// Assert that no task with the given name was executed.
    ///
    /// # Panics
    ///
    /// Panics if a task with the given name was executed.
    pub fn assert_task_not_scheduled(&self, task_name: &str) {
        assert!(
            !self.tasks_executed.contains(&task_name.to_string()),
            "Expected task '{}' to NOT be scheduled, but it was. Executed tasks: {:?}",
            task_name,
            self.tasks_executed
        );
    }

    /// Assert how many tasks were executed in total.
    ///
    /// # Panics
    ///
    /// Panics if the count differs.
    pub fn assert_task_count(&self, expected: usize) {
        assert_eq!(
            self.tasks_executed.len(),
            expected,
            "Expected {} tasks to be executed, but {} were. Tasks: {:?}",
            expected,
            self.tasks_executed.len(),
            self.tasks_executed
        );
    }

    /// Assert that exactly these tasks were executed, in this order.
    ///
    /// # Panics
    ///
    /// Panics if the executed tasks differ from `expected_order`.
    pub fn assert_task_order(&self, expected_order: &[&str]) {
        let expected: Vec<String> = expected_order.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            self.tasks_executed, expected,
            "Expected task order {:?}, but got {:?}",
            expected, self.tasks_executed
        );
    }

    /// Assert that a state key exists.
    ///
    /// # Panics
    ///
    /// Panics if the key is not found.
    pub fn assert_state_key_exists(&self, key: &str) {
        assert!(
            self.state.contains_key(key),
            "Expected state key '{}' to exist, but it was not found. Keys: {:?}",
            key,
            self.state.keys().collect::<Vec<_>>()
        );
    }

    /// Assert that some recorded command contains `command_substring`.
    ///
    /// # Panics
    ///
    /// Panics if no matching command is found.
    pub fn assert_command_generated(&self, command_substring: &str) {
        assert!(
            self.commands.iter().any(|c| c.contains(command_substring)),
            "Expected a command containing '{}', but none found. Commands: {:?}",
            command_substring,
            self.commands
        );
    }

    /// Assert that some recorded event contains `event_substring`.
    ///
    /// # Panics
    ///
    /// Panics if no matching event is found.
    pub fn assert_event_received(&self, event_substring: &str) {
        assert!(
            self.events.iter().any(|e| e.contains(event_substring)),
            "Expected an event containing '{}', but none found. Events: {:?}",
            event_substring,
            self.events
        );
    }

    /// Assert that some recorded query contains `query_substring`.
    ///
    /// # Panics
    ///
    /// Panics if no matching query is found.
    pub fn assert_query_handled(&self, query_substring: &str) {
        assert!(
            self.queries.iter().any(|q| q.contains(query_substring)),
            "Expected a query containing '{}', but none found. Queries: {:?}",
            query_substring,
            self.queries
        );
    }

    /// The failure message if the workflow failed, otherwise `None`.
    pub fn failure_message(&self) -> Option<&str> {
        match &self.status {
            ExecutionStatus::Failed(msg) => Some(msg),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_trace(status: ExecutionStatus) -> ExecutionTrace {
        ExecutionTrace {
            workflow_id: "test-wf-1".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            status,
            state: HashMap::new(),
            tasks_executed: vec![
                "validate".to_string(),
                "process".to_string(),
                "notify".to_string(),
            ],
            commands: vec!["ScheduleTask(validate)".to_string()],
            events: vec!["approval_received".to_string()],
            queries: vec!["get_status".to_string()],
        }
    }

    #[test]
    fn test_assert_completed() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_completed();
    }

    #[test]
    #[should_panic(expected = "to be completed")]
    fn test_assert_completed_fails() {
        let trace = make_trace(ExecutionStatus::Running);
        trace.assert_completed();
    }

    #[test]
    fn test_assert_failed() {
        let trace = make_trace(ExecutionStatus::Failed("oops".to_string()));
        trace.assert_failed();
    }

    #[test]
    fn test_assert_failed_with() {
        let trace = make_trace(ExecutionStatus::Failed(
            "Payment failed: card declined".to_string(),
        ));
        trace.assert_failed_with("card declined");
    }

    #[test]
    #[should_panic(expected = "contain 'timeout'")]
    fn test_assert_failed_with_wrong_message() {
        let trace = make_trace(ExecutionStatus::Failed("card declined".to_string()));
        trace.assert_failed_with("timeout");
    }

    #[test]
    fn test_assert_task_scheduled() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_task_scheduled("validate");
        trace.assert_task_scheduled("process");
    }

    #[test]
    #[should_panic(expected = "to be scheduled")]
    fn test_assert_task_scheduled_missing() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_task_scheduled("missing_task");
    }

    #[test]
    fn test_assert_task_not_scheduled() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_task_not_scheduled("refund");
    }

    #[test]
    fn test_assert_task_count() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_task_count(3);
    }

    #[test]
    fn test_assert_task_order() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_task_order(&["validate", "process", "notify"]);
    }

    #[test]
    fn test_assert_command_generated() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_command_generated("ScheduleTask");
    }

    #[test]
    fn test_assert_event_received() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_event_received("approval");
    }

    #[test]
    fn test_assert_query_handled() {
        let trace = make_trace(ExecutionStatus::Completed);
        trace.assert_query_handled("get_status");
    }

    #[test]
    fn test_failure_message() {
        let trace = make_trace(ExecutionStatus::Failed("oops".to_string()));
        assert_eq!(trace.failure_message(), Some("oops"));

        let trace2 = make_trace(ExecutionStatus::Completed);
        assert_eq!(trace2.failure_message(), None);
    }
}
