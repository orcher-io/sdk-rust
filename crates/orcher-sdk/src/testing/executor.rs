//! [`TestWorkflowExecutor`], which runs a workflow in memory and serves its tasks from
//! mocks, without an Orcher server.

use crate::error::{Error, Result};
use crate::testing::mocks::MockRegistry;
use crate::workflow::{ScheduleTaskCommand, WorkflowCommand, WorkflowContext, WorkflowExecution};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Runs a workflow in memory, serving its tasks from a [`MockRegistry`].
///
/// Each time the workflow suspends on tasks, the executor runs their mocks and
/// replays the workflow from the start with the results injected, the same way a
/// worker replays history. Only task suspensions are handled: a workflow that waits
/// on a timer, event or child workflow returns an error.
pub struct TestWorkflowExecutor {
    mocks: Arc<RwLock<MockRegistry>>,

    /// Replay limit, so a workflow that keeps scheduling tasks cannot loop forever
    max_steps: usize,

    /// Log each step through `tracing`
    verbose: bool,
}

impl TestWorkflowExecutor {
    /// Create an executor over `mocks`, with a limit of 1000 steps.
    pub fn new(mocks: Arc<RwLock<MockRegistry>>) -> Self {
        Self {
            mocks,
            max_steps: 1000,
            verbose: false,
        }
    }

    /// Set how many times the workflow may be run before the executor gives up.
    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set whether to log each step through `tracing`.
    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Run a context-aware workflow to completion, serving its tasks from mocks.
    ///
    /// Each step builds a fresh context holding every task result so far (in replay
    /// mode once there are results) and runs the workflow in it. If the workflow
    /// suspends, the executor reads the commands it scheduled from a clone of that
    /// context, runs the mock for each new task, and runs the workflow again. Tasks
    /// that already have a result are not run twice.
    ///
    /// # Errors
    ///
    /// Returns the workflow's own error, or an error if a scheduled task has no mock,
    /// the workflow suspends on something other than a task, or it takes more than
    /// the step limit.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::testing::TestWorkflowExecutor;
    /// # use orcher_sdk::testing::MockRegistry;
    /// # use orcher_sdk::workflow::{WorkflowContext, WorkflowExecution};
    /// # use orcher_sdk::prelude::*;
    /// # use std::sync::{Arc, RwLock};
    /// # async fn example() -> Result<()> {
    /// let mut registry = MockRegistry::new();
    /// registry.register("my_task".to_string(),
    ///     orcher_sdk::testing::TaskMock::Fixed {
    ///         result: serde_json::to_vec(&"result")?
    ///     }
    /// );
    ///
    /// let mocks = Arc::new(RwLock::new(registry));
    /// let mut executor = TestWorkflowExecutor::new(mocks);
    ///
    /// let execution = WorkflowExecution {
    ///     workflow_id: "test-wf-1".to_string(),
    ///     run_id: "test-run-1".to_string(),
    ///     workflow_type: "TestWorkflow".to_string(),
    ///     attempt: 1,
    ///     namespace: "test".to_string(),
    ///     task_queue: "test-queue".to_string(),
    /// };
    ///
    /// let result: String = executor.execute_workflow(
    ///     |ctx, input: String| async move {
    ///         let task_result: String = ctx.execute_task("my_task", input).await?;
    ///         Ok(task_result)
    ///     },
    ///     execution,
    ///     "input".to_string(),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_workflow<F, Fut, I, O>(
        &mut self,
        workflow_fn: F,
        execution: WorkflowExecution,
        input: I,
    ) -> Result<O>
    where
        F: Fn(WorkflowContext, I) -> Fut,
        Fut: std::future::Future<Output = Result<O>>,
        I: Clone + Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    {
        let mut step = 0;
        let mut task_results: HashMap<String, Vec<u8>> = HashMap::new();

        if self.verbose {
            tracing::info!(
                workflow_id = %execution.workflow_id,
                workflow_type = %execution.workflow_type,
                "Starting workflow execution in test mode"
            );
        }

        loop {
            step += 1;
            if step > self.max_steps {
                return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                    format!(
                        "Workflow exceeded maximum steps ({}). This might indicate an infinite loop or you need to configure max_steps higher.",
                        self.max_steps
                    ),
                )));
            }

            let context = self.create_context_with_results(
                execution.clone(),
                task_results.clone(),
                !task_results.is_empty(), // replay if we have task results
            );

            // Clones share state, so the commands the workflow schedules are readable
            // from this clone after the original has been moved into the workflow.
            let context_clone = context.clone();

            if self.verbose {
                tracing::debug!(
                    workflow_id = %execution.workflow_id,
                    step = step,
                    task_results_count = task_results.len(),
                    replaying = !task_results.is_empty(),
                    "Executing workflow step"
                );
            }

            let result = workflow_fn(context, input.clone()).await;

            match result {
                Ok(output) => {
                    if self.verbose {
                        tracing::info!(
                            workflow_id = %execution.workflow_id,
                            steps = step,
                            "Workflow completed successfully"
                        );
                    }
                    return Ok(output);
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    // Suspension is recognized by its message, which is
                    // "Workflow suspended: {reason}".
                    if error_msg.contains("Workflow suspended") {
                        let commands = context_clone.take_commands_for_test();

                        if self.verbose {
                            tracing::debug!(
                                workflow_id = %execution.workflow_id,
                                commands_count = commands.len(),
                                "Processing commands"
                            );
                        }

                        let mut has_new_tasks = false;
                        for command in commands {
                            if let WorkflowCommand::ScheduleTask(task_cmd) = command {
                                if task_results.contains_key(&task_cmd.task_id) {
                                    if self.verbose {
                                        tracing::trace!(
                                            task_id = %task_cmd.task_id,
                                            "Skipping already-executed task"
                                        );
                                    }
                                    continue;
                                }

                                has_new_tasks = true;

                                let task_result = self.execute_mock_task(&task_cmd)?;
                                task_results.insert(task_cmd.task_id.clone(), task_result);

                                if self.verbose {
                                    tracing::debug!(
                                        workflow_id = %execution.workflow_id,
                                        task_type = %task_cmd.task_type,
                                        task_id = %task_cmd.task_id,
                                        "Executed mocked task"
                                    );
                                }
                            }
                        }

                        if !has_new_tasks {
                            // Suspended with no new task: the workflow is waiting on an
                            // event, timer or child workflow, which this executor cannot
                            // resolve.
                            return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                                format!(
                                    "Workflow suspended on non-task operation at step {}. \
                                     Operations like wait_for_event, timer, or child workflows \
                                     are not yet fully supported in TestEnv. \
                                     Task execution with mocks is fully supported.",
                                    step
                                ),
                            )));
                        }

                        // Loop again with the new task results.
                    } else {
                        // A real failure of the workflow.
                        if self.verbose {
                            tracing::error!(
                                workflow_id = %execution.workflow_id,
                                error = %e,
                                step = step,
                                "Workflow execution failed"
                            );
                        }
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Run a workflow that takes no context.
    ///
    /// Such a workflow cannot call tasks, so it runs once, directly, and no mocks
    /// are involved. `_execution` is ignored.
    pub async fn execute_simple_workflow<F, Fut, I, O>(
        &mut self,
        workflow_fn: F,
        _execution: WorkflowExecution,
        input: I,
    ) -> Result<O>
    where
        F: Fn(I) -> Fut,
        Fut: std::future::Future<Output = Result<O>>,
        I: Clone + Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    {
        workflow_fn(input).await
    }

    /// Run the mock registered for a scheduled task and return its result as JSON bytes.
    fn execute_mock_task(&mut self, task_cmd: &ScheduleTaskCommand) -> Result<Vec<u8>> {
        let mut registry = self.mocks.write().unwrap();

        if !registry.has_mock(&task_cmd.task_type) {
            return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                format!(
                    "No mock registered for task '{}'. Use env.mock_task(\"{}\").returns(...) to register a mock before executing the workflow.",
                    task_cmd.task_type,
                    task_cmd.task_type
                ),
            )));
        }

        // The executor does not know the task's types, so input and output pass
        // through `serde_json::Value`.
        let input_any: serde_json::Value =
            serde_json::from_slice(&task_cmd.input).map_err(|e| {
                Error::Serialization(format!("Failed to deserialize task input: {}", e))
            })?;

        let result_any: serde_json::Value =
            registry.execute_mock(&task_cmd.task_type, input_any)?;

        let result_bytes = serde_json::to_vec(&result_any)
            .map_err(|e| Error::Serialization(format!("Failed to serialize task result: {}", e)))?;

        Ok(result_bytes)
    }

    /// Build a workflow context with `task_results` injected, keyed by task ID.
    fn create_context_with_results(
        &self,
        execution: WorkflowExecution,
        task_results: HashMap<String, Vec<u8>>,
        replaying: bool,
    ) -> WorkflowContext {
        let context = WorkflowContext::new(execution, false, "1.0.0".to_string());

        context.set_replaying_for_test(replaying);

        for (task_id, result) in task_results {
            context.inject_task_result_for_test(task_id, result);
        }

        context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks);
        assert_eq!(executor.max_steps, 1000);
        assert!(!executor.verbose);
    }

    #[test]
    fn test_executor_with_max_steps() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks).with_max_steps(500);
        assert_eq!(executor.max_steps, 500);
    }

    #[test]
    fn test_executor_with_verbose() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let executor = TestWorkflowExecutor::new(mocks).with_verbose(true);
        assert!(executor.verbose);
    }

    #[tokio::test]
    async fn test_simple_workflow_execution() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let mut executor = TestWorkflowExecutor::new(mocks);

        let execution = WorkflowExecution {
            workflow_id: "test-wf-1".to_string(),
            run_id: "test-run-1".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let workflow_fn = |input: i32| async move { Ok(input * 2) };
        let result: i32 = executor
            .execute_simple_workflow(workflow_fn, execution, 21)
            .await
            .unwrap();

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_context_workflow_without_tasks() {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        let mut executor = TestWorkflowExecutor::new(mocks);

        let execution = WorkflowExecution {
            workflow_id: "test-wf-2".to_string(),
            run_id: "test-run-2".to_string(),
            workflow_type: "ContextWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let workflow_fn = |ctx: WorkflowContext, input: String| async move {
            let wf_id = ctx.workflow_id();
            Ok(format!("{}-{}", wf_id, input))
        };
        let result: String = executor
            .execute_workflow(workflow_fn, execution.clone(), "hello".to_string())
            .await
            .unwrap();

        assert_eq!(result, format!("{}-hello", execution.workflow_id));
    }

    #[tokio::test]
    async fn test_context_cloning() {
        let execution = WorkflowExecution {
            workflow_id: "test-wf-3".to_string(),
            run_id: "test-run-3".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0.0".to_string());
        let ctx_clone = ctx.clone();

        assert_eq!(ctx.workflow_id(), ctx_clone.workflow_id());
        assert_eq!(ctx.workflow_type(), ctx_clone.workflow_type());
        assert_eq!(ctx.is_replaying(), ctx_clone.is_replaying());
    }
}
