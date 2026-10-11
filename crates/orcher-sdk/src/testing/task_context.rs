//! Building a [`TaskContext`] to unit-test a task body.
//!
//! A task is an ordinary async function taking a [`TaskContext`], so it can be
//! called directly in a test. [`TestTaskContext`] builds the context, and the
//! [`TaskProbe`] it returns lets the test cancel the task and read the
//! heartbeats it sent.
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//! use orcher_sdk::testing::TestTaskContext;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Progress {
//!     done: u32,
//! }
//!
//! async fn resize_images(ctx: TaskContext, count: u32) -> Result<u32> {
//!     for done in 1..=count {
//!         ctx.heartbeat_with_details(Some(Progress { done })).await?;
//!     }
//!     Ok(count)
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let (ctx, probe) = TestTaskContext::new().attempt(2).build();
//!
//! assert_eq!(resize_images(ctx, 3).await?, 3);
//!
//! let progress: Vec<Progress> = probe.heartbeat_details()?;
//! assert_eq!(progress.last().map(|p| p.done), Some(3));
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::task::context::HeartbeatMessage;
use crate::TaskContext;

/// Builds a [`TaskContext`] for calling a task body directly in a test.
///
/// Every field has a default, so `TestTaskContext::new().build()` is enough
/// for a task that does not read its metadata.
#[derive(Debug, Clone)]
#[must_use = "call `build` to get the context"]
pub struct TestTaskContext {
    workflow_id: String,
    run_id: String,
    task_id: String,
    attempt: i32,
    heartbeat_timeout: Option<Duration>,
}

impl Default for TestTaskContext {
    fn default() -> Self {
        Self {
            workflow_id: "test-workflow".to_string(),
            run_id: "test-run".to_string(),
            task_id: "test-task".to_string(),
            attempt: 1,
            heartbeat_timeout: None,
        }
    }
}

impl TestTaskContext {
    /// Starts from the defaults: workflow `test-workflow`, run `test-run`,
    /// task `test-task`, first attempt, no heartbeat timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the ID of the workflow the task belongs to.
    pub fn workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = workflow_id.into();
        self
    }

    /// Sets the ID of the workflow run the task belongs to.
    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = run_id.into();
        self
    }

    /// Sets the task's ID.
    pub fn task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = task_id.into();
        self
    }

    /// Sets the attempt, counted from 1, so a test can exercise retry paths
    /// such as [`TaskContext::is_retry`]. Values below 1 are taken as 1, as
    /// the worker does.
    pub fn attempt(mut self, attempt: i32) -> Self {
        self.attempt = attempt;
        self
    }

    /// Sets the heartbeat timeout the task sees through
    /// [`TaskContext::heartbeat_timeout`]. Nothing enforces it in a test.
    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    /// Builds the context, and the probe that observes it.
    pub fn build(self) -> (TaskContext, TaskProbe) {
        let (heartbeat_tx, heartbeat_rx) = mpsc::unbounded_channel();
        let cancellation = Arc::new(CancellationToken::new());
        let ctx = TaskContext::with_heartbeat_channel(
            self.workflow_id,
            self.run_id,
            self.task_id,
            self.attempt,
            self.heartbeat_timeout,
            heartbeat_tx,
            cancellation.clone(),
        );
        let probe = TaskProbe {
            cancellation,
            heartbeat_rx: Mutex::new(heartbeat_rx),
            received: Mutex::new(Vec::new()),
        };
        (ctx, probe)
    }
}

/// Observes a [`TaskContext`] built by [`TestTaskContext`]: cancels the task,
/// and reads the heartbeats it sent.
///
/// The probe keeps working after the context is dropped, so a test can call
/// the task, let it consume the context, then inspect what it did.
#[derive(Debug)]
pub struct TaskProbe {
    cancellation: Arc<CancellationToken>,
    heartbeat_rx: Mutex<mpsc::UnboundedReceiver<HeartbeatMessage>>,
    /// Details of every heartbeat received so far, in the order sent.
    received: Mutex<Vec<Option<Vec<u8>>>>,
}

impl TaskProbe {
    /// Cancels the task, as the engine does when its workflow is cancelled.
    ///
    /// [`TaskContext::is_cancelled`] then returns `true`, its cancellation
    /// token fires, and heartbeats fail with a cancellation error.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns whether the task has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns how many heartbeats the task has sent, with or without details.
    pub fn heartbeat_count(&self) -> usize {
        self.drain();
        self.received.lock().unwrap().len()
    }

    /// Returns the details of the heartbeats that carried them, decoded as
    /// `T`, in the order they were sent. Heartbeats without details are
    /// skipped.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if some details do not decode as `T`.
    pub fn heartbeat_details<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.drain();
        self.received
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .map(|bytes| {
                serde_json::from_slice(bytes).map_err(|e| {
                    Error::Serialization(format!("heartbeat details do not decode: {e}"))
                })
            })
            .collect()
    }

    /// Moves the heartbeats sent since the last call into `received`.
    fn drain(&self) {
        let mut rx = self.heartbeat_rx.lock().unwrap();
        let mut received = self.received.lock().unwrap();
        while let Ok(message) = rx.try_recv() {
            received.push(message.details);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_describe_a_first_attempt() {
        let (ctx, probe) = TestTaskContext::new().build();
        assert_eq!(ctx.workflow_id(), "test-workflow");
        assert_eq!(ctx.run_id(), "test-run");
        assert_eq!(ctx.task_id(), "test-task");
        assert_eq!(ctx.attempt(), 1);
        assert!(!ctx.is_retry());
        assert_eq!(ctx.heartbeat_timeout(), None);
        assert_eq!(probe.heartbeat_count(), 0);
    }

    #[test]
    fn metadata_is_what_the_test_set() {
        let (ctx, _probe) = TestTaskContext::new()
            .workflow_id("wf-1")
            .run_id("run-1")
            .task_id("task-1")
            .attempt(3)
            .heartbeat_timeout(Duration::from_secs(5))
            .build();
        assert_eq!(ctx.workflow_id(), "wf-1");
        assert_eq!(ctx.run_id(), "run-1");
        assert_eq!(ctx.task_id(), "task-1");
        assert_eq!(ctx.attempt(), 3);
        assert!(ctx.is_retry());
        assert_eq!(ctx.heartbeat_timeout(), Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn the_probe_reads_heartbeats_after_the_context_is_gone() {
        let (ctx, probe) = TestTaskContext::new().build();
        ctx.heartbeat().await.unwrap();
        ctx.heartbeat_with_details(Some(7u32)).await.unwrap();
        ctx.heartbeat_with_details(Some(9u32)).await.unwrap();
        drop(ctx);

        assert_eq!(probe.heartbeat_count(), 3);
        assert_eq!(probe.heartbeat_details::<u32>().unwrap(), vec![7, 9]);
        // Reading again returns the same heartbeats.
        assert_eq!(probe.heartbeat_details::<u32>().unwrap(), vec![7, 9]);
    }

    #[tokio::test]
    async fn cancelling_through_the_probe_reaches_the_task() {
        let (ctx, probe) = TestTaskContext::new().build();
        assert!(!ctx.is_cancelled());

        probe.cancel();

        assert!(probe.is_cancelled());
        assert!(ctx.is_cancelled());
        assert!(ctx.cancellation_token().is_cancelled());
        assert!(ctx.heartbeat().await.is_err());
    }

    #[test]
    fn details_of_the_wrong_type_are_reported() {
        let (ctx, probe) = TestTaskContext::new().build();
        tokio_test::block_on(ctx.heartbeat_with_details(Some("text"))).unwrap();
        assert!(matches!(
            probe.heartbeat_details::<u32>(),
            Err(Error::Serialization(_))
        ));
    }
}
