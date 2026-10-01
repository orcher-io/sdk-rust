//! Workflow APIs for defining and executing durable workflows.
//!
//! A workflow is a durable function that orchestrates tasks, timers, and other workflows.
//! Its state is persisted, and it is retried on failure.
//!
//! ## Running work
//!
//! A workflow runs work in two ways:
//! - [`WorkflowContext::execute_task`] runs a **task** registered with the worker, named by
//!   the typed reference `#[task]` generates or by string. Tasks are retried under their
//!   retry policy.
//! - [`WorkflowContext::execute`] runs an inline **closure**, for a one-off side effect
//!   such as an HTTP call or a query.
//!
//! Every result is journaled, so on replay the recorded result is returned and the work
//! does not run again.
//!
//! ## Example
//!
//! ```rust
//! use orcher::prelude::*;
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Payment {
//!     charge_id: String,
//! }
//!
//! #[task(retry = 3)]
//! async fn process_payment(_ctx: TaskContext, order_id: String) -> Result<Payment> {
//!     Ok(Payment { charge_id: format!("ch_{order_id}") })
//! }
//!
//! #[workflow]
//! async fn fulfil_order(ctx: WorkflowContext, order_id: String) -> Result<String> {
//!     // A task registered with #[task], named by its typed reference.
//!     let payment: Payment = ctx.execute_task(process_payment, order_id.clone()).await?;
//!
//!     // A durable timer.
//!     ctx.sleep(Duration::from_secs(300)).await?;
//!
//!     // An inline closure for a one-off side effect.
//!     let tracking: String = ctx
//!         .execute("create_shipment", move || async move { Ok(format!("track-{order_id}")) })
//!         .await?;
//!
//!     Ok(format!("{} shipped as {tracking}", payment.charge_id))
//! }
//! ```

pub mod closure_commands;
mod command;
mod context;
pub mod executable;
mod execution;
mod handle;
pub mod handler;
mod options;
mod rand;
mod saga;
pub mod session;
mod time;
mod traits;
pub mod update;

pub use context::WorkflowContext;
pub use executable::{ClosureExecution, Executable, TaskExecution};
pub use execution::WorkflowExecution;
pub use handle::ChildWorkflowHandle;
pub use handler::UserWorkflowFn;
pub use options::{
    ChildWorkflowOptions, OrphanPolicy, RestartFreshOptions, RetryPolicy, TaskOptions,
};
pub use rand::WorkflowRand;
pub use saga::{Saga, SagaStepBuilder};
pub use session::{SessionContext, SessionInfo, SessionOptions, SessionState};
pub use time::WorkflowTime;
pub use traits::Workflow;

// Command types are public so test harnesses can inspect what a workflow emitted.
pub use command::{
    CancelTimerCommand, CompleteWorkflowCommand, FailWorkflowCommand,
    OrphanPolicy as CommandOrphanPolicy, RecordStepResultCommand, RestartFreshCommand,
    ScheduleTaskCommand, SendEventCommand, StartChildWorkflowCommand, StartTimerCommand,
    TaskRetryPolicy, WaitForEventCommand, WorkflowCommand,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_workflow_context_metadata() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 2,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());

        assert_eq!(ctx.workflow_id(), "wf-123");
        assert_eq!(ctx.run_id(), "run-456");
        assert_eq!(ctx.workflow_type(), "TestWorkflow");
        assert_eq!(ctx.attempt(), 2);
        assert_eq!(ctx.namespace(), "default");
        assert_eq!(ctx.task_queue(), "test-queue");
        assert_eq!(ctx.version(), "1.0");
        assert!(!ctx.is_replaying());
    }

    #[test]
    fn test_workflow_context_replaying() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, true, "1.0".to_string());
        assert!(ctx.is_replaying());
    }

    #[test]
    fn test_workflow_state_management() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());

        ctx.set_state("count", &42i32).unwrap();
        ctx.set_state("name", &"test".to_string()).unwrap();

        let counter: i32 = ctx.get_state("count").unwrap().unwrap();
        assert_eq!(counter, 42);

        let name: String = ctx.get_state("name").unwrap().unwrap();
        assert_eq!(name, "test");

        // A missing key is Ok(None), not an error.
        let missing: Option<String> = ctx.get_state("missing").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_workflow_state_serialization() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct OrderData {
            id: String,
            amount: f64,
        }

        let order = OrderData {
            id: "order-123".to_string(),
            amount: 99.99,
        };

        ctx.set_state("order", &order).unwrap();

        let retrieved: OrderData = ctx.get_state("order").unwrap().unwrap();
        assert_eq!(retrieved, order);
    }

    #[test]
    fn test_sequence_counter() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());

        // The sequence counter is private; a fresh context has emitted no commands.
        assert!(ctx.take_commands().is_empty());
    }

    #[test]
    fn test_command_generation() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };

        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());

        assert!(ctx.take_commands().is_empty());
    }

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_interval, Duration::from_secs(1));
        assert_eq!(policy.max_interval, Duration::from_secs(60));
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_task_options_defaults() {
        let options = TaskOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.task_id.is_none());
        assert!(options.retry_policy.is_none());
        assert!(options.heartbeat_timeout.is_none());
    }
}
