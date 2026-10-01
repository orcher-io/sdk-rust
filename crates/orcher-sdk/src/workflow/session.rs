//! Worker sessions: running a series of tasks on the same worker.
//!
//! Use a session when later tasks depend on state held by one worker, such as
//! a model loaded into memory, a downloaded file, or open connections.
//!
//! ## How it works
//!
//! Each session gets a task queue that only one worker polls:
//! 1. `ctx.create_session()` schedules an internal session creation task.
//! 2. A worker picks it up, takes a session slot, and returns its own queue name.
//! 3. Tasks run through the returned `SessionContext` go to that queue.
//! 4. `session.complete()` releases the slot, and the worker stops polling the queue.
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//! use orcher_sdk::workflow::session::SessionOptions;
//! use std::time::Duration;
//!
//! #[task]
//! async fn load_model(_ctx: TaskContext, name: String) -> Result<String> {
//!     Ok(format!("loaded {name}"))
//! }
//!
//! #[task]
//! async fn run_inference(_ctx: TaskContext, prompt: String) -> Result<String> {
//!     Ok(format!("answer to {prompt}"))
//! }
//!
//! #[workflow]
//! async fn answer(ctx: WorkflowContext, prompt: String) -> Result<String> {
//!     let mut session = ctx
//!         .create_session(
//!             SessionOptions::default()
//!                 .with_creation_timeout(Duration::from_secs(30))
//!                 .with_execution_timeout(Duration::from_secs(600)),
//!         )
//!         .await?;
//!
//!     // Both tasks run on the same worker.
//!     let _model: String = session.execute_task(load_model, "small".to_string()).await?;
//!     let answer: String = session.execute_task(run_inference, prompt).await?;
//!
//!     session.complete();
//!     Ok(answer)
//! }
//! ```

use crate::error::{Error, Result, WorkflowError};
use crate::task::IntoTaskName;
use crate::workflow::command::{ScheduleTaskCommand, WorkflowCommand};
use crate::workflow::WorkflowContext;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;

/// Task type of the internal task that creates a session.
pub const SESSION_CREATE_TASK: &str = "__orcher_create_session";

/// Task type of the internal task that closes a session.
pub const SESSION_COMPLETE_TASK: &str = "__orcher_complete_session";

/// Separator in session queue names, which have the form
/// `{original_queue}__session__{worker_resource_id}`.
pub const SESSION_QUEUE_SEPARATOR: &str = "__session__";

/// Configuration for creating a worker session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionOptions {
    /// Maximum time to wait for a worker to accept the session.
    ///
    /// Creation fails if no worker has a free session slot within this time.
    #[serde(with = "humantime_serde_compat")]
    pub creation_timeout: Duration,

    /// Total session lifetime.
    ///
    /// The session is closed after this time even if tasks are still running.
    #[serde(with = "humantime_serde_compat")]
    pub execution_timeout: Duration,

    /// Maximum number of the session's tasks running at once.
    ///
    /// Further tasks wait in the session's queue.
    pub max_concurrent_tasks: usize,

    /// How often the session's worker reports that it is alive.
    ///
    /// If the worker dies, the engine notices within about twice this interval.
    #[serde(with = "humantime_serde_compat")]
    pub heartbeat_interval: Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            creation_timeout: Duration::from_secs(30),
            execution_timeout: Duration::from_secs(600),
            max_concurrent_tasks: 1,
            heartbeat_interval: Duration::from_secs(5),
        }
    }
}

impl SessionOptions {
    /// Sets how long to wait for a worker to accept the session.
    pub fn with_creation_timeout(mut self, timeout: Duration) -> Self {
        self.creation_timeout = timeout;
        self
    }

    /// Sets the session's total lifetime.
    pub fn with_execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = timeout;
        self
    }

    /// Sets how many of the session's tasks may run at once.
    pub fn with_max_concurrent_tasks(mut self, max: usize) -> Self {
        self.max_concurrent_tasks = max;
        self
    }

    /// Sets how often the session's worker reports that it is alive.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }
}

/// Session lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    /// Tasks can be run on the session's worker.
    Open,
    /// The workflow completed the session.
    Closed,
    /// The worker died, missed its heartbeats, or the session timed out.
    Failed,
}

/// Metadata about an active session, returned by the session creation task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    /// Unique session identifier.
    pub session_id: String,
    /// Queue that only the session's worker polls, e.g.
    /// `default__session__worker_abc123`. All session tasks go here.
    pub session_queue: String,
    /// Identity of the worker that accepted the session.
    pub worker_identity: String,
    /// Current session state.
    pub state: SessionState,
}

/// Input payload for the `__orcher_create_session` internal task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionInput {
    /// Identifier for the new session.
    pub session_id: String,
    /// [`SessionOptions::creation_timeout`], in milliseconds.
    pub creation_timeout_ms: u64,
    /// [`SessionOptions::execution_timeout`], in milliseconds.
    pub execution_timeout_ms: u64,
    /// [`SessionOptions::max_concurrent_tasks`].
    pub max_concurrent_tasks: usize,
    /// [`SessionOptions::heartbeat_interval`], in milliseconds.
    pub heartbeat_interval_ms: u64,
}

/// Input payload for the `__orcher_complete_session` internal task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteSessionInput {
    /// Session to close.
    pub session_id: String,
}

/// A workflow context that runs every task on the session's worker.
///
/// Created by [`WorkflowContext::create_session()`]. Tasks run through it are
/// sent to the session's own queue, so they all run on the same worker.
///
/// # Replay
///
/// `create_session()` uses sequence-based task IDs, so on replay the session
/// info comes from the journal and no new session is created.
pub struct SessionContext<'a> {
    ctx: &'a WorkflowContext,
    info: SessionInfo,
}

impl<'a> SessionContext<'a> {
    /// Wraps `ctx` for the session described by `info`.
    pub(crate) fn new(ctx: &'a WorkflowContext, info: SessionInfo) -> Self {
        Self { ctx, info }
    }

    /// Runs a registered task on the session's worker.
    ///
    /// Behaves like [`WorkflowContext::execute_task`], except that the task is
    /// sent to the session's queue.
    ///
    /// # Errors
    ///
    /// Returns an invalid-state error if the session is not open, and
    /// otherwise the same errors as [`WorkflowContext::execute_task`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use orcher_sdk::workflow::session::SessionContext;
    /// #[task]
    /// async fn transcode(_ctx: TaskContext, path: String) -> Result<String> {
    ///     Ok(format!("{path}.mp4"))
    /// }
    ///
    /// async fn on_session(session: &SessionContext<'_>) -> Result<String> {
    ///     session.execute_task(transcode, "/tmp/input.mov".to_string()).await
    /// }
    /// ```
    pub async fn execute_task<T, I, O>(&self, task: T, input: I) -> Result<O>
    where
        T: IntoTaskName,
        I: Serialize + Send + Sync + 'static,
        O: DeserializeOwned + Send + Sync + 'static,
    {
        if self.info.state != SessionState::Open {
            return Err(Error::Workflow(WorkflowError::InvalidState(format!(
                "Cannot execute task on session '{}': state is {:?}",
                self.info.session_id, self.info.state
            ))));
        }

        // TaskExecution consumes this override for the next task it schedules.
        {
            let mut state = self.ctx.state.lock().unwrap();
            state.task_queue_override = Some(self.info.session_queue.clone());
        }

        // Same metrics, logging and replay handling as a normal task.
        self.ctx.execute_task(task, input).await
    }

    /// Completes the session and releases the worker's session slot.
    ///
    /// The worker then stops polling the session queue and can accept new
    /// sessions. Later `execute_task` calls on this session fail. Calling this
    /// on a session that is not open only logs a warning.
    pub fn complete(&mut self) {
        if self.info.state != SessionState::Open {
            tracing::warn!(
                session_id = %self.info.session_id,
                state = ?self.info.state,
                "Attempted to complete session that is not open"
            );
            return;
        }

        self.info.state = SessionState::Closed;

        let input = CompleteSessionInput {
            session_id: self.info.session_id.clone(),
        };

        let input_bytes = serde_json::to_vec(&input).unwrap_or_default();
        let sequence = self.ctx.next_sequence();

        let command = ScheduleTaskCommand {
            sequence,
            task_id: format!("{}_{}", SESSION_COMPLETE_TASK, sequence),
            task_type: SESSION_COMPLETE_TASK.to_string(),
            task_queue: self.info.session_queue.clone(),
            input: input_bytes,
            timeout: Duration::from_secs(30),
            heartbeat_timeout: None,
            queue_timeout: None,
            retry_policy: None,
            headers: vec![],
        };

        self.ctx.add_command(WorkflowCommand::ScheduleTask(command));

        tracing::debug!(
            session_id = %self.info.session_id,
            session_queue = %self.info.session_queue,
            "Session completion scheduled"
        );
    }

    /// Returns the session metadata.
    pub fn info(&self) -> &SessionInfo {
        &self.info
    }

    /// Returns `true` while the session is open.
    pub fn is_open(&self) -> bool {
        self.info.state == SessionState::Open
    }

    /// Returns the session ID.
    pub fn session_id(&self) -> &str {
        &self.info.session_id
    }

    /// Returns the identity of the worker that owns the session.
    pub fn worker_identity(&self) -> &str {
        &self.info.worker_identity
    }

    /// Returns the name of the session's queue.
    pub fn session_queue(&self) -> &str {
        &self.info.session_queue
    }
}

/// Builds a session queue name from the original queue and a worker resource ID.
pub fn build_session_queue(original_queue: &str, worker_resource_id: &str) -> String {
    format!(
        "{}{}{}",
        original_queue, SESSION_QUEUE_SEPARATOR, worker_resource_id
    )
}

/// Returns `true` if `queue` is a session queue name.
pub fn is_session_queue(queue: &str) -> bool {
    queue.contains(SESSION_QUEUE_SEPARATOR)
}

/// Returns the original queue name from a session queue name.
///
/// A name that is not a session queue is returned unchanged.
pub fn original_queue_from_session(session_queue: &str) -> Option<&str> {
    session_queue.split(SESSION_QUEUE_SEPARATOR).next()
}

/// Serializes a `Duration` as integer milliseconds.
mod humantime_serde_compat {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        duration.as_millis().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_options_defaults() {
        let opts = SessionOptions::default();
        assert_eq!(opts.creation_timeout, Duration::from_secs(30));
        assert_eq!(opts.execution_timeout, Duration::from_secs(600));
        assert_eq!(opts.max_concurrent_tasks, 1);
        assert_eq!(opts.heartbeat_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_session_options_builder() {
        let opts = SessionOptions::default()
            .with_creation_timeout(Duration::from_secs(60))
            .with_execution_timeout(Duration::from_secs(1200))
            .with_max_concurrent_tasks(4)
            .with_heartbeat_interval(Duration::from_secs(10));

        assert_eq!(opts.creation_timeout, Duration::from_secs(60));
        assert_eq!(opts.execution_timeout, Duration::from_secs(1200));
        assert_eq!(opts.max_concurrent_tasks, 4);
        assert_eq!(opts.heartbeat_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_build_session_queue() {
        let queue = build_session_queue("default", "worker_abc123");
        assert_eq!(queue, "default__session__worker_abc123");
    }

    #[test]
    fn test_is_session_queue() {
        assert!(is_session_queue("default__session__worker_abc123"));
        assert!(!is_session_queue("default"));
        assert!(!is_session_queue("my-queue"));
    }

    #[test]
    fn test_original_queue_from_session() {
        assert_eq!(
            original_queue_from_session("default__session__worker_abc123"),
            Some("default")
        );
        assert_eq!(original_queue_from_session("default"), Some("default"));
    }

    #[test]
    fn test_session_state_transitions() {
        let info = SessionInfo {
            session_id: "session_1".to_string(),
            session_queue: "default__session__worker_abc".to_string(),
            worker_identity: "worker-1".to_string(),
            state: SessionState::Open,
        };
        assert_eq!(info.state, SessionState::Open);
    }

    #[test]
    fn test_create_session_input_serialization() {
        let input = CreateSessionInput {
            session_id: "session_1".to_string(),
            creation_timeout_ms: 30000,
            execution_timeout_ms: 600000,
            max_concurrent_tasks: 4,
            heartbeat_interval_ms: 5000,
        };

        let json = serde_json::to_string(&input).unwrap();
        let decoded: CreateSessionInput = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.session_id, "session_1");
        assert_eq!(decoded.max_concurrent_tasks, 4);
    }

    #[test]
    fn test_session_options_serialization() {
        let opts = SessionOptions::default();
        let json = serde_json::to_string(&opts).unwrap();
        let decoded: SessionOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.creation_timeout, opts.creation_timeout);
        assert_eq!(decoded.execution_timeout, opts.execution_timeout);
    }
}
