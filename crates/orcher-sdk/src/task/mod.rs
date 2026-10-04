//! Tasks: the units of work a workflow schedules.
//!
//! A task is where a workflow touches the outside world: it can do I/O, call
//! external services, write to databases and have other side effects. Unlike
//! workflow code, a task does not have to be deterministic. Its result is
//! recorded, so a workflow that replays does not run the task again; a task
//! that fails is retried under its [`RetryPolicy`].
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! // The #[task] macro is the usual way to define a task; this is the shape
//! // of one written by hand.
//! struct ProcessPaymentTask;
//!
//! impl ProcessPaymentTask {
//!     async fn execute(ctx: &TaskContext, order_id: String) -> Result<PaymentResult> {
//!         // Long-running tasks heartbeat so the engine knows they are alive.
//!         ctx.heartbeat().await?;
//!
//!         // Tasks may do I/O, call databases, and so on.
//!         let result = process_payment_external(&order_id).await?;
//!
//!         Ok(result)
//!     }
//! }
//! # async fn process_payment_external(id: &str) -> Result<PaymentResult> { todo!() }
//! # struct PaymentResult;
//! ```

mod context;
mod options;
mod reference;
mod retry;
mod traits;

pub use context::{TaskContext, TaskLogger};
pub use options::{TaskExecutionOptions, TaskInfo};
pub use reference::{IntoTaskName, TaskReference};
pub use retry::{retry_with_policy, JitterStrategy, RetryPolicy};
pub use traits::Task;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn test_task_context_creation() {
        let ctx = TaskContext::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            Some(Duration::from_secs(30)),
        );

        assert_eq!(ctx.workflow_id(), "wf-123");
        assert_eq!(ctx.run_id(), "run-456");
        assert_eq!(ctx.task_id(), "task-789");
        assert_eq!(ctx.attempt(), 1);
        assert_eq!(ctx.heartbeat_timeout(), Some(Duration::from_secs(30)));
        assert!(!ctx.is_retry());
        assert!(!ctx.is_cancelled());
    }

    #[test]
    fn test_task_context_retry() {
        let ctx = TaskContext::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            3,
            None,
        );

        assert_eq!(ctx.attempt(), 3);
        assert!(ctx.is_retry());
    }

    /// An attempt the engine does not number (0) reads as the first, never
    /// as attempt 0.
    #[test]
    fn test_task_context_attempt_counts_from_one() {
        for (sent, read) in [(0, 1), (-1, 1), (1, 1), (2, 2)] {
            let ctx = TaskContext::new(
                "wf-123".to_string(),
                "run-456".to_string(),
                "task-789".to_string(),
                sent,
                None,
            );
            assert_eq!(ctx.attempt(), read, "attempt {sent} sent");
            assert_eq!(ctx.is_retry(), read > 1, "attempt {sent} sent");
            assert_eq!(ctx.logger().attempt(), read, "attempt {sent} sent");
        }
    }

    #[test]
    fn test_task_logger() {
        let ctx = TaskContext::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            None,
        );

        let logger = ctx.logger();
        assert_eq!(logger.workflow_id(), "wf-123");
        assert_eq!(logger.run_id(), "run-456");
        assert_eq!(logger.task_id(), "task-789");
        assert_eq!(logger.attempt(), 1);
        assert_eq!(logger.target(), "orcher_sdk::task::task-789");
    }

    #[tokio::test]
    async fn test_heartbeat_without_channel() {
        let ctx = TaskContext::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            Some(Duration::from_secs(30)),
        );

        // Heartbeat without channel should succeed (testing mode)
        let result = ctx.heartbeat().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_heartbeat_with_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancellation_token = Arc::new(tokio_util::sync::CancellationToken::new());

        let ctx = TaskContext::with_heartbeat_channel(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            Some(Duration::from_secs(30)),
            tx,
            cancellation_token,
        );

        // Send heartbeat
        ctx.heartbeat().await.unwrap();

        // Verify message was sent
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.task_id, "task-789");
        assert!(msg.details.is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_with_details() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancellation_token = Arc::new(tokio_util::sync::CancellationToken::new());

        let ctx = TaskContext::with_heartbeat_channel(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            None,
            tx,
            cancellation_token,
        );

        #[derive(serde::Serialize, serde::Deserialize)]
        struct Progress {
            current: u32,
            total: u32,
        }

        // Send heartbeat with details
        ctx.heartbeat_with_details(Some(Progress {
            current: 50,
            total: 100,
        }))
        .await
        .unwrap();

        // Verify message was sent with details
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.task_id, "task-789");
        assert!(msg.details.is_some());

        // Verify details can be deserialized
        let details: Progress = serde_json::from_slice(&msg.details.unwrap()).unwrap();
        assert_eq!(details.current, 50);
        assert_eq!(details.total, 100);
    }

    #[tokio::test]
    async fn test_cancellation_detection() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancellation_token = Arc::new(tokio_util::sync::CancellationToken::new());

        let ctx = TaskContext::with_heartbeat_channel(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            None,
            tx,
            Arc::clone(&cancellation_token),
        );

        // Initially not cancelled
        assert!(!ctx.is_cancelled());

        // Trigger cancellation
        cancellation_token.cancel();

        // Now should be cancelled
        assert!(ctx.is_cancelled());

        // Heartbeat should fail with cancellation error
        let result = ctx.heartbeat().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::Error::Task(crate::error::TaskError::Canceled)
        ));
    }

    #[tokio::test]
    async fn test_cancellation_token_access() {
        let ctx = TaskContext::new(
            "wf-123".to_string(),
            "run-456".to_string(),
            "task-789".to_string(),
            1,
            None,
        );

        let token = ctx.cancellation_token();

        // Should be able to use in tokio::select
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                // Work completed
            }
            _ = token.cancelled() => {
                // Cancelled
                panic!("Should not be cancelled");
            }
        }
    }
}
