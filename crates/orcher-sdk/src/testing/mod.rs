//! Testing utilities for Orcher workflows.
//!
//! Workflows run in memory, without an Orcher server. Tasks are replaced by mocks, and a
//! run's status, state and history can be inspected and asserted on.
//!
//! # Quick Start
//!
//! [`TestWorkflowExecutor`] runs a workflow and answers the tasks it schedules from
//! mocks, so a test checks the workflow's logic without running any task code:
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//! use orcher_sdk::testing::{MockRegistry, MockTaskBuilder, TestWorkflowExecutor};
//! use orcher_sdk::workflow::WorkflowExecution;
//! use std::sync::{Arc, RwLock};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Order {
//!     id: String,
//!     amount_cents: u64,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Receipt {
//!     order_id: String,
//!     charge_id: String,
//! }
//!
//! async fn checkout(ctx: WorkflowContext, order: Order) -> Result<Receipt> {
//!     let charge_id: String = ctx.execute_task("charge_card", order.clone()).await?;
//!     Ok(Receipt { order_id: order.id, charge_id })
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let mocks = Arc::new(RwLock::new(MockRegistry::new()));
//! MockTaskBuilder::new("charge_card".to_string(), mocks.clone()).returns("ch_123");
//!
//! let execution = WorkflowExecution::new(
//!     "order-1".to_string(),
//!     "run-1".to_string(),
//!     "checkout".to_string(),
//!     "default".to_string(),
//!     "orders".to_string(),
//! );
//! let order = Order { id: "order-1".to_string(), amount_cents: 4_200 };
//!
//! let receipt: Receipt = TestWorkflowExecutor::new(mocks)
//!     .execute_workflow(checkout, execution, order)
//!     .await?;
//! assert_eq!(receipt.charge_id, "ch_123");
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! ## In-Memory Execution
//!
//! [`TestEnv`] registers workflows by type name and runs them in memory, with no
//! Orcher server:
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//! use orcher_sdk::testing::TestEnv;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let mut env = TestEnv::new();
//! env.register_workflow("greet", |name: String| async move {
//!     Ok(format!("Hello, {name}!"))
//! });
//!
//! let greeting: String = env.execute_workflow("greet", "Ada").await?;
//! assert_eq!(greeting, "Hello, Ada!");
//! # Ok(())
//! # }
//! ```
//!
//! ## Task Mocking
//!
//! Register a mock for each task a workflow calls, in the registry the
//! [`TestWorkflowExecutor`] reads. A mock returns a fixed value, a sequence of values,
//! the result of a function, or an error:
//!
//! ```rust
//! use orcher_sdk::testing::{MockRegistry, MockTaskBuilder};
//! use serde::{Deserialize, Serialize};
//! use std::sync::{Arc, RwLock};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Item {
//!     price: f64,
//! }
//!
//! let mocks = Arc::new(RwLock::new(MockRegistry::new()));
//! let mock = |task: &str| MockTaskBuilder::new(task.to_string(), mocks.clone());
//!
//! // The same value on every call (pass the success value, not a `Result`).
//! mock("validate_order").returns(true);
//!
//! // Computed from the input; the function receives and returns JSON bytes.
//! mock("calculate_total").with_fn(|input| {
//!     let items: Vec<Item> = serde_json::from_slice(&input)
//!         .map_err(|e| orcher_sdk::Error::Serialization(e.to_string()))?;
//!     let total: f64 = items.iter().map(|item| item.price).sum();
//!     serde_json::to_vec(&total).map_err(|e| orcher_sdk::Error::Serialization(e.to_string()))
//! });
//!
//! // One value per call, in order.
//! mock("fetch_page").returns_seq(vec![1, 2, 3]);
//!
//! // A failure on every call.
//! mock("flaky_service").fails_with("service unavailable");
//!
//! assert!(mocks.read().unwrap().has_mock("fetch_page"));
//! ```
//!
//! ## Assertions
//!
//! Assertions panic with a message that shows what was found instead. They take a
//! blocking lock, so call them outside the async runtime:
//!
//! ```rust
//! # use orcher_sdk::testing::TestEnv;
//! # fn example(env: &TestEnv, workflow_id: &str) {
//! // Assert task was called
//! env.assert_task_called("process_payment", 1);
//!
//! // Assert workflow completed
//! env.assert_workflow_completed(workflow_id);
//!
//! // Assert state value
//! env.assert_state_equals(workflow_id, "status", &"completed".to_string());
//! # }
//! ```
//!
//! # Architecture
//!
//! [`TestEnv`] registers and runs workflows and records each run's status in an
//! execution trace. [`MockRegistry`] holds the task mocks. [`TestWorkflowExecutor`]
//! runs a context-aware workflow against a mock registry: each time the workflow
//! suspends on tasks, it runs their mocks and replays the workflow with the results.
//! [`MockClock`] is a manually advanced clock for time-dependent test code.
//!
//! # Limitations
//!
//! - [`TestEnv::execute_workflow`] does not consult task mocks: a workflow that calls
//!   `ctx.execute_task` suspends, and the run returns an error. Use
//!   [`TestWorkflowExecutor`] for workflows that call tasks.
//! - `TestEnv` does not record state, executed tasks or task calls from a run, so the
//!   state and task inspection methods find nothing for it.
//! - [`TestWorkflowExecutor`] handles task suspensions only. A workflow that waits on a
//!   timer, event or child workflow returns an error.
//! - Task code never runs, and everything runs in one process with no persistence.
//!
//! For integration tests with real execution, run against an Orcher server.

mod assertions;
mod env;
mod executor;
mod mocks;
mod time;
mod trace;

pub use assertions::{
    assert_state_equals, assert_state_exists, assert_task_called, assert_task_called_at_least,
    assert_task_not_called, assert_tasks_executed, assert_tasks_executed_in_order,
    assert_workflow_completed, assert_workflow_failed,
};
pub use env::{ExecutionStatus, ExecutionTrace, TaskCall, TestEnv};
pub use executor::TestWorkflowExecutor;
pub use mocks::{MockRegistry, MockTask, MockTaskBuilder, TaskMock};
pub use time::MockClock;
// `trace` exports nothing: it adds assertion methods to `ExecutionTrace` in an `impl` block.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_testing_module_available() {
        let _env = TestEnv::new();
    }
}
