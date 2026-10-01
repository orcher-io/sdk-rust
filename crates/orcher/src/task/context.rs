//! The context passed to a task while it runs.

use crate::error::{Error, Result};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;

/// The context a task runs in.
///
/// Through it a task reports progress with heartbeats, learns that it has been
/// canceled, reads metadata about the attempt, and gets a logger.
///
/// ## Heartbeats
///
/// The worker heartbeats every task it runs on its own, at a third of the
/// task's heartbeat timeout, so a long task needs no heartbeat code to stay
/// alive, and one whose worker dies is timed out and retried. A task calls
/// `heartbeat` to report progress: the details are sent with the next
/// heartbeat due, and handed to its next attempt should this one fail. The
/// heartbeat's answer is also how the engine asks a task to stop; see
/// [`is_cancelled`](Self::is_cancelled).
///
/// ```rust
/// # use orcher::prelude::*;
/// async fn long_task(ctx: &TaskContext, items: Vec<String>) -> Result<()> {
///     for (i, item) in items.iter().enumerate() {
///         // Process item
///         process_item(item).await?;
///
///         // Report progress every 10 items.
///         if i % 10 == 0 {
///             ctx.heartbeat().await?;
///         }
///     }
///     Ok(())
/// }
/// # async fn process_item(item: &str) -> Result<()> { Ok(()) }
/// ```
pub struct TaskContext {
    workflow_id: String,

    run_id: String,

    task_id: String,

    /// Starts at 1.
    attempt: i32,

    heartbeat_timeout: Option<Duration>,

    /// Channel to a heartbeat consumer, for contexts not run by the task driver.
    heartbeat_tx: Option<mpsc::UnboundedSender<HeartbeatMessage>>,

    /// Canceled when the task should stop.
    cancellation_token: Arc<tokio_util::sync::CancellationToken>,

    /// The task's heartbeat from the worker's task driver, which sends what
    /// `heartbeat` records and cancels `cancellation_token` when the engine
    /// asks the task to stop.
    driver_heartbeat: Option<orcher_sdk_core::poller::TaskHeartbeat>,
}

/// A heartbeat sent through `heartbeat_tx`.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used internally for heartbeat communication
pub(crate) struct HeartbeatMessage {
    /// The task sending the heartbeat.
    pub task_id: String,

    /// Progress details, serialized as JSON.
    pub details: Option<Vec<u8>>,

    /// When the heartbeat was sent.
    pub timestamp: SystemTime,
}

impl TaskContext {
    /// Create a task context with no heartbeat consumer.
    #[cfg_attr(not(test), allow(dead_code))] // The worker builds one with its heartbeat.
    pub(crate) fn new(
        workflow_id: String,
        run_id: String,
        task_id: String,
        attempt: i32,
        heartbeat_timeout: Option<Duration>,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            task_id,
            attempt,
            heartbeat_timeout,
            heartbeat_tx: None,
            cancellation_token: Arc::new(tokio_util::sync::CancellationToken::new()),
            driver_heartbeat: None,
        }
    }

    /// Create a task context for a task the worker's driver is running, which
    /// heartbeats it on its own: `heartbeat` sends what the task records, and
    /// its cancellation token is the context's.
    pub(crate) fn with_heartbeat(
        workflow_id: String,
        run_id: String,
        task_id: String,
        attempt: i32,
        heartbeat_timeout: Option<Duration>,
        heartbeat: orcher_sdk_core::poller::TaskHeartbeat,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            task_id,
            attempt,
            heartbeat_timeout,
            heartbeat_tx: None,
            cancellation_token: Arc::new(heartbeat.cancellation_token()),
            driver_heartbeat: Some(heartbeat),
        }
    }

    /// Create a task context that sends heartbeats through `heartbeat_tx`.
    #[allow(dead_code)] // Used by service executor for heartbeat support
    pub(crate) fn with_heartbeat_channel(
        workflow_id: String,
        run_id: String,
        task_id: String,
        attempt: i32,
        heartbeat_timeout: Option<Duration>,
        heartbeat_tx: mpsc::UnboundedSender<HeartbeatMessage>,
        cancellation_token: Arc<tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            task_id,
            attempt,
            heartbeat_timeout,
            heartbeat_tx: Some(heartbeat_tx),
            cancellation_token,
            driver_heartbeat: None,
        }
    }

    // ============================================================================
    // Heartbeat
    // ============================================================================

    /// Send a heartbeat to indicate the task is still running.
    ///
    /// The worker already heartbeats the task on its own while it runs; this
    /// adds one from the task's code, folded into the same stream: it never
    /// waits, and however often it is called no more heartbeats are sent than
    /// the worker's timer sends. Fails once the task has been cancelled.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// async fn my_task(ctx: &TaskContext) -> Result<()> {
    ///     for i in 0..100 {
    ///         // Do some work
    ///         heavy_operation(i).await?;
    ///
    ///         // Report progress on every iteration.
    ///         ctx.heartbeat().await?;
    ///     }
    ///     Ok(())
    /// }
    /// # async fn heavy_operation(i: i32) -> Result<()> { Ok(()) }
    /// ```
    pub async fn heartbeat(&self) -> Result<()> {
        self.heartbeat_with_details::<()>(None).await
    }

    /// Send a heartbeat with optional progress details.
    ///
    /// The details can be any serializable value. They are handed to the next
    /// attempt if this one fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # use serde::Serialize;
    /// #[derive(Serialize)]
    /// struct Progress {
    ///     current: u32,
    ///     total: u32,
    /// }
    ///
    /// async fn my_task(ctx: &TaskContext) -> Result<()> {
    ///     let total = 100;
    ///     for current in 0..total {
    ///         // Do work
    ///         process_item(current).await?;
    ///
    ///         // Report how far the task has got.
    ///         ctx.heartbeat_with_details(Some(Progress { current, total })).await?;
    ///     }
    ///     Ok(())
    /// }
    /// # async fn process_item(i: u32) -> Result<()> { Ok(()) }
    /// ```
    pub async fn heartbeat_with_details<T>(&self, details: Option<T>) -> Result<()>
    where
        T: Serialize,
    {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::Task(crate::error::TaskError::Canceled));
        }

        let details_bytes = match details {
            Some(d) => {
                let bytes = serde_json::to_vec(&d).map_err(|e| {
                    Error::Serialization(format!("Failed to serialize heartbeat details: {}", e))
                })?;
                Some(bytes)
            }
            None => None,
        };

        // Handed to the driver, which sends it with the next heartbeat due.
        if let Some(heartbeat) = &self.driver_heartbeat {
            heartbeat.record(details_bytes);
        } else if let Some(tx) = &self.heartbeat_tx {
            let has_details = details_bytes.is_some();
            let message = HeartbeatMessage {
                task_id: self.task_id.clone(),
                details: details_bytes,
                timestamp: SystemTime::now(),
            };

            // The channel is unbounded, so this never blocks.
            tx.send(message).map_err(|_| {
                Error::Task(crate::error::TaskError::HeartbeatFailed(
                    "Heartbeat channel closed".to_string(),
                ))
            })?;

            tracing::debug!(
                task_id = %self.task_id,
                workflow_id = %self.workflow_id,
                has_details = has_details,
                "Heartbeat sent"
            );
        } else {
            // No consumer (for example in tests): the heartbeat is dropped.
            tracing::trace!(
                task_id = %self.task_id,
                "Heartbeat called but no channel configured (testing mode)"
            );
        }

        Ok(())
    }

    // ============================================================================
    // Metadata
    // ============================================================================

    /// Get the workflow ID this task belongs to.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the workflow run ID.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Get the unique task ID.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Get the current attempt number, starting at 1.
    ///
    /// Useful for behaving differently on retries.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }

    /// Get the heartbeat timeout, if configured.
    pub fn heartbeat_timeout(&self) -> Option<Duration> {
        self.heartbeat_timeout
    }

    /// Check if this is a retry attempt.
    pub fn is_retry(&self) -> bool {
        self.attempt > 1
    }

    /// Check if the task has been cancelled.
    ///
    /// A long-running task should check this periodically and return early
    /// once it is set.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// use orcher::error::TaskError;
    ///
    /// async fn long_task(ctx: &TaskContext) -> Result<()> {
    ///     for i in 0..1000 {
    ///         // Check for cancellation
    ///         if ctx.is_cancelled() {
    ///             return Err(Error::Task(TaskError::Canceled));
    ///         }
    ///
    ///         // Do work...
    ///         process_item(i).await?;
    ///     }
    ///     Ok(())
    /// }
    /// # async fn process_item(i: i32) -> Result<()> { Ok(()) }
    /// ```
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Get the task's cancellation token.
    ///
    /// Use it with `tokio::select!` to stop waiting on work as soon as the
    /// task is canceled.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// use orcher::error::TaskError;
    ///
    /// async fn cancellable_task(ctx: &TaskContext) -> Result<()> {
    ///     let token = ctx.cancellation_token();
    ///     let work = async {
    ///         // Do long-running work
    ///         tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    ///         Ok(())
    ///     };
    ///
    ///     tokio::select! {
    ///         result = work => result,
    ///         _ = token.cancelled() => {
    ///             Err(Error::Task(TaskError::Canceled))
    ///         }
    ///     }
    /// }
    /// ```
    pub fn cancellation_token(&self) -> Arc<tokio_util::sync::CancellationToken> {
        Arc::clone(&self.cancellation_token)
    }

    // ============================================================================
    // Logging
    // ============================================================================

    /// Get a logger for this task.
    ///
    /// The logger carries the task's metadata.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// async fn my_task(ctx: &TaskContext) -> Result<()> {
    ///     let logger = ctx.logger();
    ///     tracing::info!(
    ///         workflow_id = logger.workflow_id(),
    ///         task_id = logger.task_id(),
    ///         attempt = logger.attempt(),
    ///         "task started"
    ///     );
    ///     Ok(())
    /// }
    /// ```
    pub fn logger(&self) -> TaskLogger {
        TaskLogger {
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            task_id: self.task_id.clone(),
            attempt: self.attempt,
        }
    }
}

/// Task metadata for structured logging, from [`TaskContext::logger`].
pub struct TaskLogger {
    workflow_id: String,
    run_id: String,
    task_id: String,
    attempt: i32,
}

impl TaskLogger {
    /// Get the logging target for this task.
    pub fn target(&self) -> String {
        format!("orcher::task::{}", self.task_id)
    }

    /// Get the workflow ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the run ID.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Get the task ID.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Get the attempt number.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }
}
