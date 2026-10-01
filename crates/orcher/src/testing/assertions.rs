//! Free-function assertions over a [`TestEnv`]. Each one panics with a descriptive
//! message when the check fails.

use crate::testing::TestEnv;
use serde::de::DeserializeOwned;

// These helpers exist for user tests and are unused inside the crate, hence the
// `dead_code` allows.
#[allow(dead_code)]
/// Assert that a task was called exactly `expected_count` times.
///
/// # Panics
///
/// Panics if the count differs.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_task_called};
///
/// # fn example(env: &TestEnv) {
/// assert_task_called(env, "process_payment", 1);
/// # }
/// ```
pub fn assert_task_called(env: &TestEnv, task_type: &str, expected_count: usize) {
    env.assert_task_called(task_type, expected_count);
}

/// Assert that a workflow completed successfully.
///
/// # Panics
///
/// Panics if the workflow did not complete or is not found.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_workflow_completed};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_workflow_completed(env, workflow_id);
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_workflow_completed(env: &TestEnv, workflow_id: &str) {
    env.assert_workflow_completed(workflow_id);
}

/// Assert that a workflow state key holds the expected value.
///
/// # Panics
///
/// Panics if the key is missing or the value differs.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_state_equals};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_state_equals(env, workflow_id, "status", &"completed".to_string());
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_state_equals<T>(env: &TestEnv, workflow_id: &str, key: &str, expected: &T)
where
    T: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    env.assert_state_equals(workflow_id, key, expected);
}

/// Assert that a workflow failed.
///
/// # Panics
///
/// Panics if the workflow did not fail or is not found.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_workflow_failed};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_workflow_failed(env, workflow_id);
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_workflow_failed(env: &TestEnv, workflow_id: &str) {
    use crate::testing::env::ExecutionStatus;

    let status = env.workflow_status(workflow_id);
    assert!(
        matches!(status, Some(ExecutionStatus::Failed(_))),
        "Expected workflow '{}' to have failed, but status was {:?}",
        workflow_id,
        status
    );
}

/// Assert that a task was called at least `min_count` times.
///
/// # Panics
///
/// Panics if the task was called fewer times.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_task_called_at_least};
///
/// # fn example(env: &TestEnv) {
/// assert_task_called_at_least(env, "retry_task", 3);
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_task_called_at_least(env: &TestEnv, task_type: &str, min_count: usize) {
    let actual_count = env.task_call_count(task_type);
    assert!(
        actual_count >= min_count,
        "Expected task '{}' to be called at least {} times, but was called {} times",
        task_type,
        min_count,
        actual_count
    );
}

/// Assert that a task was never called.
///
/// # Panics
///
/// Panics if the task was called.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_task_not_called};
///
/// # fn example(env: &TestEnv) {
/// assert_task_not_called(env, "deprecated_task");
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_task_not_called(env: &TestEnv, task_type: &str) {
    assert_task_called(env, task_type, 0);
}

/// Assert that a workflow state key exists.
///
/// # Panics
///
/// Panics if the key does not exist. The message lists the keys that do.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_state_exists};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_state_exists(env, workflow_id, "order_id");
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_state_exists(env: &TestEnv, workflow_id: &str, key: &str) {
    let keys = env.workflow_state_keys(workflow_id);
    assert!(
        keys.contains(&key.to_string()),
        "Expected workflow '{}' to have state key '{}', but it was not found. Available keys: {:?}",
        workflow_id,
        key,
        keys
    );
}

/// Assert that a workflow executed exactly these tasks, in this order.
///
/// # Panics
///
/// Panics if the number of executed tasks differs or any position does not match.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_tasks_executed_in_order};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_tasks_executed_in_order(env, workflow_id, &["validate", "process", "ship"]);
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_tasks_executed_in_order(env: &TestEnv, workflow_id: &str, expected_order: &[&str]) {
    let executed = env.executed_tasks(workflow_id);

    assert_eq!(
        executed.len(),
        expected_order.len(),
        "Expected {} tasks to be executed, but {} were executed",
        expected_order.len(),
        executed.len()
    );

    for (i, expected) in expected_order.iter().enumerate() {
        assert_eq!(
            executed[i], *expected,
            "Expected task at position {} to be '{}', but was '{}'",
            i, expected, executed[i]
        );
    }
}

/// Assert that a workflow executed each of these tasks, in any order.
///
/// Other tasks may also have run.
///
/// # Panics
///
/// Panics if any of the expected tasks was not executed.
///
/// # Example
///
/// ```rust
/// use orcher::testing::{TestEnv, assert_tasks_executed};
///
/// # fn example(env: &TestEnv, workflow_id: &str) {
/// assert_tasks_executed(env, workflow_id, &["validate", "process", "ship"]);
/// # }
/// ```
#[allow(dead_code)] // Public API for user tests
pub fn assert_tasks_executed(env: &TestEnv, workflow_id: &str, expected_tasks: &[&str]) {
    let executed = env.executed_tasks(workflow_id);

    for expected in expected_tasks {
        assert!(
            executed.contains(&expected.to_string()),
            "Expected task '{}' to be executed, but it was not. Executed tasks: {:?}",
            expected,
            executed
        );
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_assertions_module_compiles() {
        // Passes if the module compiles.
    }
}
