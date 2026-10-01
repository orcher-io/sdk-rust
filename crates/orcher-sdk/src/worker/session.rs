//! Worker-side session management.
//!
//! A session pins a series of tasks to one worker by routing them to a queue
//! only that worker polls. The [`SessionManager`] caps how many sessions a
//! worker hosts, tells the `TaskDriver` which session queues to poll, and backs
//! the built-in `__orcher_create_session` and `__orcher_complete_session` tasks.

use crate::error::{Error, Result};
use crate::workflow::session::{
    build_session_queue, CompleteSessionInput, CreateSessionInput, SessionInfo, SessionState,
};
use dashmap::DashMap;
pub use orcher_sdk_core::poller::SessionQueueChange;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn};

/// Tracks the sessions hosted by one worker.
///
/// Each worker has one manager, which limits how many sessions it hosts at
/// once. Creating a session takes a slot, derives the session queue name from
/// the worker's resource ID, and asks the `TaskDriver` to poll that queue.
/// Completing it releases the slot and stops the polling.
pub struct SessionManager {
    /// Random ID of this worker instance, generated at startup. Session queues
    /// are named `{original_queue}__session__{resource_id}`.
    resource_id: String,

    /// Maximum number of concurrent sessions this worker can host.
    max_sessions: usize,

    /// One permit per session slot.
    session_slots: Arc<Semaphore>,

    /// Active sessions by session ID.
    active_sessions: Arc<DashMap<String, SessionEntry>>,

    /// Tells the `TaskDriver` to start or stop polling a session queue.
    queue_change_tx: mpsc::UnboundedSender<SessionQueueChange>,
}

/// An active session on this worker.
#[derive(Debug, Clone)]
pub struct SessionEntry {
    /// Identifier of the session.
    pub session_id: String,
    /// Worker-specific queue the session's tasks are routed to.
    pub session_queue: String,
    /// Queue the session was created from.
    pub original_queue: String,
    /// Cancelled when the session ends; stops its heartbeat loop and releases
    /// its slot.
    pub cancel: tokio_util::sync::CancellationToken,
}

/// Configuration for the worker's session manager.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionManagerConfig {
    /// Maximum concurrent sessions this worker can host.
    pub max_sessions: usize,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self { max_sessions: 10 }
    }
}

impl SessionManager {
    /// Creates a session manager with a fresh resource ID.
    ///
    /// `queue_change_tx` is the `TaskDriver`'s channel for session queue changes.
    pub fn new(
        config: SessionManagerConfig,
        queue_change_tx: mpsc::UnboundedSender<SessionQueueChange>,
    ) -> Self {
        let resource_id = uuid::Uuid::new_v4().to_string();

        info!(
            resource_id = %resource_id,
            max_sessions = config.max_sessions,
            "SessionManager created"
        );

        Self {
            resource_id,
            max_sessions: config.max_sessions,
            session_slots: Arc::new(Semaphore::new(config.max_sessions)),
            active_sessions: Arc::new(DashMap::new()),
            queue_change_tx,
        }
    }

    /// Returns this worker's resource ID.
    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }

    /// Returns the number of currently active sessions.
    pub fn active_session_count(&self) -> usize {
        self.active_sessions.len()
    }

    /// Returns the number of available session slots.
    pub fn available_slots(&self) -> usize {
        self.session_slots.available_permits()
    }

    /// Handles the built-in `__orcher_create_session` task.
    ///
    /// Takes a session slot without waiting, asks the `TaskDriver` to poll the
    /// new session queue, and returns the [`SessionInfo`] that becomes the task
    /// result. When `heartbeat_tx` is given, a loop sends a [`HeartbeatRequest`]
    /// on it every `heartbeat_interval_ms` until the session ends.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::ResourceExhausted`] if every slot is in use.
    ///
    /// [`WorkerError::ResourceExhausted`]: crate::error::WorkerError::ResourceExhausted
    pub async fn handle_create_session(
        &self,
        input: CreateSessionInput,
        task_queue: &str,
        heartbeat_tx: Option<mpsc::UnboundedSender<HeartbeatRequest>>,
    ) -> Result<SessionInfo> {
        // Fail at once when no slot is free rather than waiting for one.
        let permit = self
            .session_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Error::Worker(crate::error::WorkerError::ResourceExhausted {
                    resource: format!(
                        "session slots ({}/{} in use)",
                        self.max_sessions - self.session_slots.available_permits(),
                        self.max_sessions,
                    ),
                })
            })?;

        let session_queue = build_session_queue(task_queue, &self.resource_id);
        let cancel = tokio_util::sync::CancellationToken::new();

        let entry = SessionEntry {
            session_id: input.session_id.clone(),
            session_queue: session_queue.clone(),
            original_queue: task_queue.to_string(),
            cancel: cancel.clone(),
        };

        self.active_sessions.insert(input.session_id.clone(), entry);

        if let Err(e) = self
            .queue_change_tx
            .send(SessionQueueChange::Add(session_queue.clone()))
        {
            warn!(
                session_id = %input.session_id,
                error = %e,
                "Failed to notify TaskDriver of new session queue"
            );
        }

        info!(
            session_id = %input.session_id,
            session_queue = %session_queue,
            resource_id = %self.resource_id,
            available_slots = self.session_slots.available_permits(),
            "Session created"
        );

        if let Some(hb_tx) = heartbeat_tx {
            let session_id = input.session_id.clone();
            let interval_ms = input.heartbeat_interval_ms;
            let cancel_clone = cancel.clone();

            tokio::spawn(async move {
                let interval = std::time::Duration::from_millis(interval_ms);
                loop {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => {
                            debug!(session_id = %session_id, "Session heartbeat loop stopped");
                            break;
                        }
                        _ = tokio::time::sleep(interval) => {
                            if hb_tx.send(HeartbeatRequest {
                                session_id: session_id.clone(),
                            }).is_err() {
                                warn!(session_id = %session_id, "Heartbeat channel closed");
                                break;
                            }
                        }
                    }
                }
            });
        }

        // Hold the slot for the session's lifetime: a task owns the permit and
        // drops it when the session is cancelled.
        let cancel_for_permit = cancel.clone();
        tokio::spawn(async move {
            cancel_for_permit.cancelled().await;
            drop(permit);
        });

        Ok(SessionInfo {
            session_id: input.session_id,
            session_queue,
            worker_identity: self.resource_id.clone(),
            state: SessionState::Open,
        })
    }

    /// Handles the built-in `__orcher_complete_session` task.
    ///
    /// Releases the session slot, stops the heartbeat loop, and stops polling the
    /// session queue. Completing an unknown session only logs a warning.
    pub async fn handle_complete_session(&self, input: CompleteSessionInput) -> Result<()> {
        if let Some((_, entry)) = self.active_sessions.remove(&input.session_id) {
            // Stops the heartbeat loop and releases the slot.
            entry.cancel.cancel();

            if let Err(e) = self
                .queue_change_tx
                .send(SessionQueueChange::Remove(entry.session_queue.clone()))
            {
                warn!(
                    session_id = %input.session_id,
                    error = %e,
                    "Failed to notify TaskDriver to remove session queue"
                );
            }

            info!(
                session_id = %input.session_id,
                session_queue = %entry.session_queue,
                available_slots = self.session_slots.available_permits() + 1,
                "Session completed"
            );
        } else {
            warn!(
                session_id = %input.session_id,
                "Attempted to complete unknown session"
            );
        }

        Ok(())
    }

    /// Whether the session is active on this worker.
    pub fn is_session_active(&self, session_id: &str) -> bool {
        self.active_sessions.contains_key(session_id)
    }

    /// Returns the entry for an active session.
    pub fn get_session(&self, session_id: &str) -> Option<SessionEntry> {
        self.active_sessions.get(session_id).map(|e| e.clone())
    }

    /// Closes every active session, for example during worker shutdown.
    ///
    /// Releases all slots and stops polling every session queue.
    pub fn close_all_sessions(&self) {
        let sessions: Vec<_> = self
            .active_sessions
            .iter()
            .map(|e| e.value().clone())
            .collect();

        for entry in &sessions {
            entry.cancel.cancel();
            let _ = self
                .queue_change_tx
                .send(SessionQueueChange::Remove(entry.session_queue.clone()));
        }

        self.active_sessions.clear();

        if !sessions.is_empty() {
            info!(
                count = sessions.len(),
                "Force-closed all active sessions during shutdown"
            );
        }
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        // Stop heartbeat loops and release slots; the queue changes are not
        // sent, since the driver is going away too.
        for entry in self.active_sessions.iter() {
            entry.value().cancel.cancel();
        }
    }
}

/// Liveness signal sent by a session's heartbeat loop.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    /// Session whose liveness is being reported.
    pub session_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_manager_create_and_complete() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let manager = SessionManager::new(SessionManagerConfig { max_sessions: 2 }, tx);

        assert_eq!(manager.available_slots(), 2);
        assert_eq!(manager.active_session_count(), 0);

        let input = CreateSessionInput {
            session_id: "session_1".to_string(),
            creation_timeout_ms: 30000,
            execution_timeout_ms: 600000,
            max_concurrent_tasks: 1,
            heartbeat_interval_ms: 5000,
        };

        let info = manager
            .handle_create_session(input, "default", None)
            .await
            .unwrap();

        assert_eq!(info.session_id, "session_1");
        assert!(info.session_queue.contains("__session__"));
        assert_eq!(info.state, SessionState::Open);
        assert_eq!(manager.active_session_count(), 1);

        // The driver is told to poll the session queue.
        let change = rx.recv().await.unwrap();
        match change {
            SessionQueueChange::Add(queue) => assert_eq!(queue, info.session_queue),
            _ => panic!("Expected Add"),
        }

        let complete_input = CompleteSessionInput {
            session_id: "session_1".to_string(),
        };
        manager
            .handle_complete_session(complete_input)
            .await
            .unwrap();

        assert_eq!(manager.active_session_count(), 0);

        // And to stop polling it.
        let change = rx.recv().await.unwrap();
        match change {
            SessionQueueChange::Remove(queue) => assert_eq!(queue, info.session_queue),
            _ => panic!("Expected Remove"),
        }
    }

    #[tokio::test]
    async fn test_session_manager_slot_exhaustion() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SessionManager::new(SessionManagerConfig { max_sessions: 1 }, tx);

        // The first session takes the only slot.
        let input1 = CreateSessionInput {
            session_id: "session_1".to_string(),
            creation_timeout_ms: 30000,
            execution_timeout_ms: 600000,
            max_concurrent_tasks: 1,
            heartbeat_interval_ms: 5000,
        };
        manager
            .handle_create_session(input1, "default", None)
            .await
            .unwrap();

        // The second finds no slot free.
        let input2 = CreateSessionInput {
            session_id: "session_2".to_string(),
            creation_timeout_ms: 30000,
            execution_timeout_ms: 600000,
            max_concurrent_tasks: 1,
            heartbeat_interval_ms: 5000,
        };
        let result = manager.handle_create_session(input2, "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_manager_close_all() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = SessionManager::new(SessionManagerConfig { max_sessions: 5 }, tx);

        for i in 0..3 {
            let input = CreateSessionInput {
                session_id: format!("session_{}", i),
                creation_timeout_ms: 30000,
                execution_timeout_ms: 600000,
                max_concurrent_tasks: 1,
                heartbeat_interval_ms: 5000,
            };
            manager
                .handle_create_session(input, "default", None)
                .await
                .unwrap();
        }

        assert_eq!(manager.active_session_count(), 3);
        manager.close_all_sessions();
        assert_eq!(manager.active_session_count(), 0);
    }

    #[test]
    fn test_session_queue_naming() {
        let resource_id = "abc123";
        let queue = build_session_queue("my-queue", resource_id);
        assert_eq!(queue, "my-queue__session__abc123");
    }
}
