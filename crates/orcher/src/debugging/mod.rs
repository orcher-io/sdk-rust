//! Tools for inspecting workflow execution while debugging and testing.
//!
//! - [`ExecutionTracer`] records events, commands and state accesses per workflow.
//! - [`CommandInspector`] lists the commands a workflow context has produced.
//! - [`StateSnapshot`] captures workflow state and diffs it against another snapshot.
//!
//! These are diagnostic aids. They sit outside the replay path and do not affect
//! how a workflow executes.
//!
//! # Quick start
//!
//! Give a [`TestEnv`](crate::testing::TestEnv) an enabled tracer, run a workflow, and
//! read what the tracer recorded. Clones of a tracer share their traces:
//!
//! ```rust
//! use orcher::debugging::ExecutionTracer;
//! use orcher::prelude::*;
//! use orcher::testing::TestEnv;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let tracer = ExecutionTracer::new().enable();
//! let mut env = TestEnv::new().with_tracer(tracer.clone());
//! env.register_workflow("greet", |name: String| async move { Ok(format!("Hello, {name}")) });
//!
//! let _: String = env.execute_workflow("greet", "Ada").await?;
//!
//! let trace = &tracer.all_traces()[0];
//! assert_eq!(trace.workflow_type, "greet");
//! assert!(!trace.has_errors());
//! # Ok(())
//! # }
//! ```
//!
//! # Execution tracing
//!
//! A tracer records nothing until it is enabled.
//!
//! ```rust
//! use orcher::debugging::{ExecutionTracer, TraceLevel};
//!
//! let tracer = ExecutionTracer::new()
//!     .with_level(TraceLevel::Debug)
//!     .with_events(true)
//!     .enable();
//! assert!(tracer.is_enabled());
//! ```
//!
//! # Command inspection
//!
//! List the commands a workflow has generated so far, in order:
//!
//! ```rust
//! use orcher::debugging::CommandInspector;
//! use orcher::workflow::{WorkflowContext, WorkflowExecution};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let execution = WorkflowExecution::new(
//!     "order-1".to_string(),
//!     "run-1".to_string(),
//!     "checkout".to_string(),
//!     "default".to_string(),
//!     "orders".to_string(),
//! );
//! let ctx = WorkflowContext::new(execution, false, "1.0.0".to_string());
//!
//! // With no recorded result, scheduling a task suspends the workflow.
//! let charged: orcher::Result<String> = ctx.execute_task("charge_card", 4_200).await;
//! assert!(charged.is_err());
//!
//! for cmd in CommandInspector::new().inspect(&ctx) {
//!     println!("{} at seq {}: {}", cmd.command_type, cmd.sequence, cmd.description());
//! }
//! # }
//! ```
//!
//! # State snapshots
//!
//! Record state values in snapshots and compare them:
//!
//! ```rust
//! use orcher::debugging::StateSnapshot;
//!
//! let mut before = StateSnapshot::new("order-1").with_label("before");
//! before.set("counter", &1)?;
//!
//! let mut after = StateSnapshot::new("order-1").with_label("after");
//! after.set("counter", &2)?;
//! after.set("status", &"shipped")?;
//!
//! let counter: Option<i32> = after.get("counter")?;
//! assert_eq!(counter, Some(2));
//!
//! let diff = before.diff(&after);
//! assert_eq!(diff.changed_keys(), ["counter"]);
//! assert_eq!(diff.added_keys(), ["status"]);
//! # Ok::<(), orcher::Error>(())
//! ```
//!
//! # Structured logging
//!
//! Workflows and tasks can log through the `tracing` crate directly:
//!
//! ```rust
//! use tracing::{info, debug, instrument};
//! use orcher::prelude::*;
//!
//! #[instrument(skip(ctx))]
//! async fn my_workflow(ctx: WorkflowContext, order_id: String) -> Result<String> {
//!     info!(order_id = %order_id, "Starting workflow");
//!
//!     let result: String = ctx.execute_task("process", order_id).await?;
//!     debug!(result = %result, "Task completed");
//!
//!     Ok(result)
//! }
//! ```

mod command_inspector;
mod execution_tracer;
mod state_snapshot;

pub use command_inspector::{CommandDetails, CommandInfo, CommandInspector, CommandSummary};
pub use execution_tracer::{ExecutionEvent, ExecutionTrace, ExecutionTracer, TraceLevel};
pub use state_snapshot::{StateDiff, StateSnapshot};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debugging_module_available() {
        let _tracer = ExecutionTracer::new();
    }
}
