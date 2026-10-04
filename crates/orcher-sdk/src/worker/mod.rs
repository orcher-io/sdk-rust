//! Workers: processes that poll the orchestrator and run workflows and tasks.
//!
//! A [`Worker`] polls one task queue, runs the registered workflow and task
//! handlers within its concurrency limits, reports results, and shuts down
//! without dropping results it already holds.
//!
//! ## Example
//!
//! ```rust,no_run
//! use orcher_sdk::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Configure a worker for one task queue.
//!     let service = Worker::builder()
//!         .server_url("http://localhost:50051")
//!         .namespace("default")
//!         .task_queue("my-queue")
//!         .max_concurrent_workflows(10)
//!         .max_concurrent_tasks(20)
//!         .build()
//!         .await?;
//!
//!     // Runs until shutdown.
//!     service.run().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! Polling and all gRPC traffic live in sdk-core's drivers (`WorkflowDriver`,
//! `TaskDriver`). This crate only runs handlers, through [`ExecutionRuntime`].
//! Keeping the two apart lets core overhead be measured separately from
//! handler time and lets handlers run as native Rust.

mod builder;
pub mod runtime;
pub mod session;

#[cfg(test)]
mod replay_tests;

#[cfg(feature = "auto-register")]
pub mod registry;

#[cfg(not(feature = "auto-register"))]
mod registry;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[cfg(feature = "auto-register")]
pub mod registration;

pub use builder::WorkerBuilder;
pub use registry::Registry;
pub use runtime::{ExecutionRuntime, RuntimeConfig, RuntimeStats};

use crate::interceptor::InterceptorChain;

use crate::error::{Error, Result};

/// How long [`Worker::run`] waits for the workflow and task drivers to stop
/// once asked.
///
/// Each driver sends the results it already holds and, by default, gives
/// completions still being retried five more seconds, so it normally stops well
/// within this bound. The bound exists for a driver stuck elsewhere: shutdown
/// must return even then.
const DRIVER_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Waits up to [`DRIVER_STOP_TIMEOUT`] for a driver that has been asked to
/// stop, and aborts it if it has not stopped by then.
async fn stop_driver(driver: &str, handle: &mut tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(DRIVER_STOP_TIMEOUT, &mut *handle).await {
        Ok(_) => tracing::debug!("{driver} driver stopped"),
        Err(_) => {
            tracing::warn!(
                timeout_ms = DRIVER_STOP_TIMEOUT.as_millis() as u64,
                "{driver} driver did not stop in time; abandoning it"
            );
            handle.abort();
        }
    }
}

use orcher_sdk_core::poller::{
    TaskDriver, TaskDriverConfig, TaskWorkResult, WorkerRegistrationConfig,
    WorkerRegistrationDriver, WorkflowDriver, WorkflowDriverConfig, WorkflowWorkResult,
};

#[cfg(feature = "auto-register")]
use orcher_sdk_core::poller::{ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorWorkResult};

/// Health status of a worker, as reported by [`Worker::get_health`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Operating normally.
    Healthy,
    /// Running, but with problems such as an elevated error rate.
    Degraded,
    /// Not functioning properly.
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "HEALTHY"),
            Self::Degraded => write!(f, "DEGRADED"),
            Self::Unhealthy => write!(f, "UNHEALTHY"),
        }
    }
}

/// Health report for a worker.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall health status.
    pub status: HealthStatus,
    /// Human-readable explanation of the status.
    pub message: String,
    /// Execution statistics the status was derived from.
    pub stats: RuntimeStats,
}

/// Runs workflows and tasks for one task queue.
///
/// A worker polls the orchestrator for work, executes the registered workflow
/// and task handlers, and reports results back. sdk-core's `WorkflowDriver` and
/// `TaskDriver` do the polling and gRPC; [`ExecutionRuntime`] stores and runs
/// the handlers.
///
/// Create one with [`WorkerBuilder`], start it with [`run()`](Worker::run), and
/// stop it gracefully with [`shutdown()`](Worker::shutdown).
///
/// ## Example
///
/// ```rust,no_run
/// # use orcher_sdk::prelude::*;
/// # async fn example() -> Result<()> {
/// let service = Worker::builder()
///     .server_url("http://localhost:50051")
///     .task_queue("orders")
///     .max_concurrent_workflows(50)
///     .build()
///     .await?;
///
/// service.run().await?;
/// # Ok(())
/// # }
/// ```
pub struct Worker {
    config: WorkerConfig,

    /// Owns the handlers and executes them.
    runtime: Arc<ExecutionRuntime>,

    /// Workflow, task and actor handlers registered with this worker.
    registry: Arc<RwLock<Registry>>,

    /// Actor client that registers (and re-registers) actor handlers.
    /// Heartbeats are sent by the `ActorDriver`.
    #[cfg(feature = "auto-register")]
    actor_client: Option<Arc<tokio::sync::Mutex<crate::actor::ActorClient>>>,

    /// Invokes actor operation handlers.
    #[cfg(feature = "auto-register")]
    actor_executor: Option<Arc<crate::actor::ActorExecutor>>,

    /// Registration ID returned when actor handlers were registered; the
    /// actor driver's heartbeat refers to it.
    #[cfg(feature = "auto-register")]
    actor_registration_id: Option<String>,

    /// ID of this worker instance, sent at worker registration.
    #[allow(dead_code)]
    service_id: String,

    /// Broadcasting on this tells every listener to stop.
    shutdown_tx: tokio::sync::broadcast::Sender<()>,

    /// When the worker was created; the baseline for health checks before
    /// the first success.
    started_at: std::time::Instant,

    /// Hooks run around every execution, for logging, metrics and tracing.
    interceptor_chain: Arc<InterceptorChain>,
}

/// Configuration for a [`Worker`].
#[derive(Clone)]
#[non_exhaustive]
pub struct WorkerConfig {
    /// Server URL, for example `http://localhost:50051`.
    pub server_url: String,

    /// Namespace (default: `"default"`).
    pub namespace: String,

    /// Task queue to poll.
    pub task_queue: String,

    /// Identity reported to the server, for observability.
    pub identity: String,

    /// Maximum concurrent workflow executions (default: 100).
    pub max_concurrent_workflows: usize,

    /// Maximum concurrent task executions (default: 200).
    pub max_concurrent_tasks: usize,

    /// Number of concurrent workflow pollers (default: 4)
    pub workflow_poller_count: usize,

    /// Number of concurrent task pollers (default: 4)
    pub task_poller_count: usize,

    /// Number of concurrent actor operation pollers (default: 4)
    pub actor_poller_count: usize,

    /// Maximum concurrent actor operation executions (default: 100)
    pub max_concurrent_actor_operations: usize,

    /// Interval between polls for work (default: 100 ms).
    pub poll_interval: Duration,

    /// Interval of the worker and actor registration heartbeats (default: 10 seconds).
    pub heartbeat_interval: Duration,

    /// Organization to act for (optional).
    ///
    /// When set, requests carry it in the `X-Organization-Id` header, which the
    /// server uses for organization-level quotas and billing attribution.
    pub organization_id: Option<String>,

    /// API key that authenticates with the server (optional).
    ///
    /// When set, every gRPC request carries an `authorization: Bearer <key>` header.
    /// Obtain a key by registering a service with the orchestrator.
    pub api_key: Option<String>,

    /// Codec chain that decodes compressed or encrypted payloads (optional).
    ///
    /// When set, workflow and task inputs are decoded with it before they reach
    /// handlers. It must match the codec chain of the client that started the
    /// workflow.
    pub codec_chain: Option<Arc<orcher_sdk_core::codec::CodecChain>>,

    /// The code release this worker is running (optional).
    ///
    /// Sent on every poll and at registration; the server binds an execution to
    /// the version that first claims it. The value is opaque: it is never parsed
    /// or ordered. Defaults to the `ORCHER_VERSION_ID` environment variable.
    pub version_id: Option<String>,

    /// Maximum number of concurrent worker sessions (default: 10).
    ///
    /// Sessions pin a series of tasks to a single worker. Each session
    /// occupies one slot; when all slots are in use, `create_session()` calls
    /// wait until a slot is released or the creation timeout expires.
    pub max_sessions: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let identity = format!("orcher-rust-{}", uuid::Uuid::new_v4());
        Self {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            identity,
            max_concurrent_workflows: 100,
            max_concurrent_tasks: 200,
            workflow_poller_count: 4,
            task_poller_count: 4,
            actor_poller_count: 4,
            max_concurrent_actor_operations: 100,
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(10),
            organization_id: None,
            api_key: None,
            codec_chain: None,
            // The release is normally set by the deployment, so read it from
            // the environment rather than requiring a builder call.
            version_id: std::env::var("ORCHER_VERSION_ID")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            max_sessions: 10,
        }
    }
}

impl WorkerConfig {
    /// Creates the default configuration, which reads `ORCHER_VERSION_ID`
    /// from the environment.
    pub fn from_env() -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("server_url", &self.server_url)
            .field("namespace", &self.namespace)
            .field("task_queue", &self.task_queue)
            .field("identity", &self.identity)
            .field("max_concurrent_workflows", &self.max_concurrent_workflows)
            .field("max_concurrent_tasks", &self.max_concurrent_tasks)
            .field("workflow_poller_count", &self.workflow_poller_count)
            .field("task_poller_count", &self.task_poller_count)
            .field("actor_poller_count", &self.actor_poller_count)
            .field(
                "max_concurrent_actor_operations",
                &self.max_concurrent_actor_operations,
            )
            .field("poll_interval", &self.poll_interval)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("organization_id", &self.organization_id)
            .field("api_key", &self.api_key.as_ref().map(|_| "***"))
            .field(
                "codec_chain",
                &self
                    .codec_chain
                    .as_ref()
                    .map(|c| format!("<CodecChain len={}>", c.len())),
            )
            .field("version_id", &self.version_id)
            .finish()
    }
}

impl Worker {
    /// Returns a [`WorkerBuilder`], the usual way to create a worker.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = Worker::builder();
    /// ```
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// The execution runtime, for registering handlers directly.
    pub fn runtime(&self) -> &Arc<ExecutionRuntime> {
        &self.runtime
    }

    /// The registry of workflow and task handlers, for inspection.
    pub fn registry(&self) -> &Arc<RwLock<Registry>> {
        &self.registry
    }

    /// The actor executor, if actors are registered.
    #[cfg(feature = "auto-register")]
    pub fn actor_executor(&self) -> Option<&Arc<crate::actor::ActorExecutor>> {
        self.actor_executor.as_ref()
    }

    /// The worker's configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Returns a snapshot of execution counts: executed, succeeded and failed,
    /// for workflows and tasks.
    pub fn get_stats(&self) -> RuntimeStats {
        self.runtime.stats()
    }

    /// Returns the worker's health, derived from its error rate and the time
    /// since its last success.
    ///
    /// - **Healthy**: no executions yet, or an error rate of at most 10% and a
    ///   success within the last 30 s.
    /// - **Degraded**: error rate above 10%, or no success for over 30 s.
    /// - **Unhealthy**: error rate above 50%, or no success for over 60 s.
    pub fn get_health(&self) -> HealthReport {
        let stats = self.runtime.stats();
        let total = stats.workflows_executed + stats.tasks_executed;
        let failures = stats.workflows_failed + stats.tasks_failed;

        // Before the first success, measure from when the worker was created.
        let last_success = self.runtime.last_success_time();
        let secs_since_success = last_success
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(self.started_at.elapsed().as_secs());

        let (status, message) = if total == 0 {
            (HealthStatus::Healthy, "No executions yet".to_string())
        } else {
            let error_rate = failures as f64 / total as f64;

            if error_rate > 0.5 || secs_since_success > 60 {
                (
                    HealthStatus::Unhealthy,
                    format!(
                        "Error rate: {:.1}%, last success: {}s ago",
                        error_rate * 100.0,
                        secs_since_success
                    ),
                )
            } else if error_rate > 0.1 || secs_since_success > 30 {
                (
                    HealthStatus::Degraded,
                    format!(
                        "Error rate: {:.1}%, last success: {}s ago",
                        error_rate * 100.0,
                        secs_since_success
                    ),
                )
            } else {
                (HealthStatus::Healthy, "Operating normally".to_string())
            }
        };

        HealthReport {
            status,
            message,
            stats,
        }
    }

    /// Requests a graceful shutdown.
    ///
    /// Signals [`run()`](Self::run) to stop the drivers, which first send the
    /// results they already hold. Call it from another task or a signal handler
    /// while `run()` is active; it returns immediately.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # async fn example(service: std::sync::Arc<Worker>) -> Result<()> {
    /// // From a signal handler or another task:
    /// service.shutdown();
    /// # Ok(())
    /// # }
    /// ```
    pub fn shutdown(&self) {
        tracing::info!("Shutdown requested");
        let _ = self.shutdown_tx.send(());
    }

    /// Runs the worker until shutdown is requested or a driver or execution
    /// loop stops.
    ///
    /// Polls for workflow and task work, executes the registered handlers,
    /// reports results, registers the worker and sends its heartbeats, and
    /// serves actor operations when actors are registered. On the way out the
    /// drivers are stopped first, so the results they hold are delivered before
    /// the execution loops end.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::StartupFailed`] if a driver cannot be created.
    ///
    /// [`WorkerError::StartupFailed`]: crate::error::WorkerError::StartupFailed
    pub async fn run(&self) -> Result<()> {
        tracing::info!(
            identity = %self.config.identity,
            namespace = %self.config.namespace,
            task_queue = %self.config.task_queue,
            "Starting ORCHER Worker (driver mode)"
        );

        self.register_handlers_from_registry().await?;

        let mut workflow_driver_config = WorkflowDriverConfig::default();
        workflow_driver_config.server_url = self.config.server_url.clone();
        workflow_driver_config.namespace = self.config.namespace.clone();
        workflow_driver_config.task_queue = self.config.task_queue.clone();
        workflow_driver_config.max_concurrent_executions = self.config.max_concurrent_workflows;
        workflow_driver_config.identity = self.config.identity.clone();
        workflow_driver_config.poll_timeout = Duration::from_secs(60);
        workflow_driver_config.cache_capacity = 1000;
        workflow_driver_config.strict_determinism = false;
        workflow_driver_config.poller_count = self.config.workflow_poller_count;
        workflow_driver_config.organization_id = self.config.organization_id.clone();
        workflow_driver_config.api_key = self.config.api_key.clone();
        workflow_driver_config.tls_config = None;
        workflow_driver_config.version_id = self.config.version_id.clone();

        let (workflow_driver, mut workflow_work_rx, workflow_result_tx) =
            WorkflowDriver::new(workflow_driver_config)
                .await
                .map_err(|e| {
                    Error::Worker(crate::error::WorkerError::StartupFailed(format!(
                        "Failed to create workflow driver: {}",
                        e
                    )))
                })?;

        let mut task_driver_config = TaskDriverConfig::default();
        task_driver_config.server_url = self.config.server_url.clone();
        task_driver_config.namespace = self.config.namespace.clone();
        task_driver_config.task_queue = self.config.task_queue.clone();
        task_driver_config.max_concurrent_executions = self.config.max_concurrent_tasks;
        task_driver_config.identity = self.config.identity.clone();
        task_driver_config.poll_timeout = Duration::from_secs(60);
        task_driver_config.enable_heartbeat = true;
        task_driver_config.heartbeat_interval = Duration::from_secs(30);
        task_driver_config.poller_count = self.config.task_poller_count;
        task_driver_config.organization_id = self.config.organization_id.clone();
        task_driver_config.api_key = self.config.api_key.clone();
        task_driver_config.tls_config = None;
        task_driver_config.version_id = self.config.version_id.clone();
        // Heartbeat every task while it runs, whatever its code does, so a
        // task whose worker dies is retried instead of staying started.
        task_driver_config.auto_heartbeat = true;

        let (mut task_driver, mut task_work_rx, task_result_tx, session_queue_tx) =
            TaskDriver::new(task_driver_config).await.map_err(|e| {
                Error::Worker(crate::error::WorkerError::StartupFailed(format!(
                    "Failed to create task driver: {}",
                    e
                )))
            })?;

        let workflow_runtime = Arc::clone(&self.runtime);
        let task_runtime = Arc::clone(&self.runtime);
        let workflow_interceptors = Arc::clone(&self.interceptor_chain);
        let task_interceptors = Arc::clone(&self.interceptor_chain);

        // The actor driver also sends the actor registration heartbeat.
        #[cfg(feature = "auto-register")]
        let actor_driver_parts = if self.actor_client.is_some() {
            let mut actor_driver_config = ActorDriverConfig::default();
            actor_driver_config.server_url = self.config.server_url.clone();
            actor_driver_config.namespace = self.config.namespace.clone();
            actor_driver_config.service_id = self.config.identity.clone();
            actor_driver_config.max_concurrent_executions =
                self.config.max_concurrent_actor_operations;
            actor_driver_config.identity = self.config.identity.clone();
            actor_driver_config.poll_timeout = Duration::from_secs(30);
            actor_driver_config.poller_count = self.config.actor_poller_count;
            actor_driver_config.organization_id = self.config.organization_id.clone();
            actor_driver_config.api_key = self.config.api_key.clone();
            actor_driver_config.enable_heartbeat = true;
            actor_driver_config.heartbeat_interval = self.config.heartbeat_interval;
            actor_driver_config.registration_id = self.actor_registration_id.clone();
            actor_driver_config.tls_config = None;

            let (driver, work_rx, result_tx, event_rx) =
                ActorDriver::new(actor_driver_config).await.map_err(|e| {
                    Error::Worker(crate::error::WorkerError::StartupFailed(format!(
                        "Failed to create actor driver: {}",
                        e
                    )))
                })?;

            Some((driver, work_rx, result_tx, event_rx))
        } else {
            None
        };

        // Eager task injection: tasks scheduled by a workflow completion go
        // straight into the task driver's work channel, skipping a poll round
        // trip. They are heartbeated like polled tasks.
        let mut workflow_driver =
            workflow_driver.with_eager_task_injector(task_driver.eager_task_injector());

        // Sessions are served by two built-in task handlers.
        let session_manager = Arc::new(session::SessionManager::new(
            session::SessionManagerConfig {
                max_sessions: self.config.max_sessions,
            },
            session_queue_tx,
        ));

        {
            let sm = Arc::clone(&session_manager);
            let task_queue = self.config.task_queue.clone();
            let handler: crate::worker::registration::TaskHandlerFn = Arc::new(
                move |_ctx: crate::TaskContext,
                      input: crate::payload::Payload|
                      -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = std::result::Result<
                                    crate::payload::Payload,
                                    crate::error::Error,
                                >,
                            > + Send,
                    >,
                > {
                    let sm = Arc::clone(&sm);
                    let task_queue = task_queue.clone();
                    Box::pin(async move {
                        let create_input: crate::workflow::session::CreateSessionInput =
                            serde_json::from_slice(&input.data).map_err(|e| {
                                crate::error::Error::Serialization(format!(
                                    "Failed to deserialize CreateSessionInput: {}",
                                    e
                                ))
                            })?;

                        let info = sm
                            .handle_create_session(create_input, &task_queue, None)
                            .await?;

                        let result_bytes = serde_json::to_vec(&info).map_err(|e| {
                            crate::error::Error::Serialization(format!(
                                "Failed to serialize SessionInfo: {}",
                                e
                            ))
                        })?;

                        Ok(crate::payload::Payload {
                            data: result_bytes,
                            metadata: std::collections::HashMap::new(),
                        })
                    })
                },
            );

            self.runtime
                .register_task_handler(crate::workflow::session::SESSION_CREATE_TASK, handler)
                .await;
        }

        {
            let sm = Arc::clone(&session_manager);
            let handler: crate::worker::registration::TaskHandlerFn = Arc::new(
                move |_ctx: crate::TaskContext,
                      input: crate::payload::Payload|
                      -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = std::result::Result<
                                    crate::payload::Payload,
                                    crate::error::Error,
                                >,
                            > + Send,
                    >,
                > {
                    let sm = Arc::clone(&sm);
                    Box::pin(async move {
                        let complete_input: crate::workflow::session::CompleteSessionInput =
                            serde_json::from_slice(&input.data).map_err(|e| {
                                crate::error::Error::Serialization(format!(
                                    "Failed to deserialize CompleteSessionInput: {}",
                                    e
                                ))
                            })?;

                        sm.handle_complete_session(complete_input).await?;

                        Ok(crate::payload::Payload {
                            data: b"null".to_vec(),
                            metadata: std::collections::HashMap::new(),
                        })
                    })
                },
            );

            self.runtime
                .register_task_handler(crate::workflow::session::SESSION_COMPLETE_TASK, handler)
                .await;
        }

        tracing::info!(
            resource_id = %session_manager.resource_id(),
            "Session manager initialized with built-in task handlers"
        );

        // Take the stop handles before the drivers move into their tasks. `run`
        // borrows a driver while it runs, and stopping it through the handle
        // lets it send the results it holds instead of dropping them.
        let wf_stop = workflow_driver.shutdown_handle();
        let task_stop = task_driver.shutdown_handle();

        let mut workflow_driver_handle = tokio::spawn(async move {
            if let Err(e) = workflow_driver.run().await {
                tracing::error!(error = %e, "Workflow driver error");
            }
        });

        let mut task_driver_handle = tokio::spawn(async move {
            if let Err(e) = task_driver.run().await {
                tracing::error!(error = %e, "Task driver error");
            }
        });

        // Workflow execution loop: one spawned task per activation.
        let mut workflow_exec_handle = tokio::spawn(async move {
            while let Some(work) = workflow_work_rx.recv().await {
                let runtime = Arc::clone(&workflow_runtime);
                let result_tx = workflow_result_tx.clone();
                let interceptors = Arc::clone(&workflow_interceptors);

                tokio::spawn(async move {
                    let task = work.task;
                    let workflow_id = task.execution.workflow_id.clone();
                    let run_id = task.execution.run_id.clone();
                    let workflow_type = task.workflow_type.clone();
                    let task_token = task.task_token.clone();
                    let task_queue = task.task_queue.clone();
                    let attempt = task.attempt;
                    let stream_entry_id = work.stream_entry_id;

                    let ictx = orcher_sdk_core::interceptor::InterceptorContext::workflow(
                        &workflow_id,
                        &workflow_type,
                        attempt as u32,
                        "default",
                        &task_queue,
                    );
                    interceptors.before_execution(&ictx);

                    let start = std::time::Instant::now();
                    let execution_result = runtime.execute_workflow(task).await;
                    let duration_ms = start.elapsed().as_millis() as u64;

                    if execution_result.successful {
                        interceptors.after_execution(&ictx, duration_ms);
                    } else {
                        let err_msg = execution_result
                            .error
                            .as_ref()
                            .map(|e| e.message.as_str())
                            .unwrap_or("unknown error");
                        interceptors.on_error(&ictx, err_msg, duration_ms);
                    }

                    let result = WorkflowWorkResult {
                        workflow_id,
                        run_id,
                        task_token,
                        stream_entry_id,
                        result: Ok(execution_result),
                    };

                    if let Err(e) = result_tx.send(result).await {
                        tracing::error!(error = %e, "Failed to send workflow result to driver");
                    }
                });
            }
        });

        // Task execution loop: one spawned task per task.
        let mut task_exec_handle = tokio::spawn(async move {
            while let Some(work) = task_work_rx.recv().await {
                let runtime = Arc::clone(&task_runtime);
                let result_tx = task_result_tx.clone();
                let interceptors = Arc::clone(&task_interceptors);

                tokio::spawn(async move {
                    let task = work.task;
                    // Hold this until the result is handed back: dropping it
                    // stops the task's heartbeats.
                    let heartbeat = work.heartbeat;
                    let task_id = task.task_id.clone();
                    let task_type = task.task_type.clone();
                    let task_queue = task.task_queue.clone();
                    // Counted from 1, as the task's context reports it.
                    let attempt = task.attempt.max(1);
                    let task_token = task.task_token.clone();

                    let ictx = orcher_sdk_core::interceptor::InterceptorContext::task(
                        &task_id,
                        &task_type,
                        attempt as u32,
                        "default",
                        &task_queue,
                    );
                    interceptors.before_execution(&ictx);

                    let start = std::time::Instant::now();
                    let execution_result = runtime.execute_task(task, heartbeat.clone()).await;
                    let duration_ms = start.elapsed().as_millis() as u64;

                    match &execution_result {
                        Ok(_) => interceptors.after_execution(&ictx, duration_ms),
                        Err(e) => interceptors.on_error(&ictx, &e.to_string(), duration_ms),
                    }

                    let result = TaskWorkResult {
                        task_token,
                        result: execution_result,
                    };

                    if let Err(e) = result_tx.send(result).await {
                        tracing::error!(error = %e, "Failed to send task result to driver");
                    }
                    drop(heartbeat);
                });
            }
        });

        #[cfg(feature = "auto-register")]
        let (actor_driver_handle, actor_exec_handle, actor_event_handle) = if let Some((
            mut driver,
            mut work_rx,
            result_tx,
            mut event_rx,
        )) =
            actor_driver_parts
        {
            let actor_executor = self.actor_executor.clone();
            let server_url = self.config.server_url.clone();

            let driver_handle = tokio::spawn(async move {
                if let Err(e) = driver.run().await {
                    tracing::error!(error = %e, "Actor driver error");
                }
            });

            let exec_handle = tokio::spawn(async move {
                while let Some(work) = work_rx.recv().await {
                    let result_tx = result_tx.clone();
                    let executor = actor_executor.clone();
                    let server_url = server_url.clone();

                    tokio::spawn(async move {
                        let operation = work.operation;
                        let operation_id = operation.operation_id.clone();
                        let execution_id = operation.execution_id.clone();

                        let result =
                            Self::execute_actor_op(executor, &operation, &server_url).await;

                        let work_result = ActorWorkResult {
                            operation_id,
                            execution_id,
                            result,
                        };

                        if let Err(e) = result_tx.send(work_result).await {
                            tracing::error!(error = %e, "Failed to send actor result to driver");
                        }
                    });
                }
            });

            // React to driver events, such as a server request to re-register.
            let actor_client_for_events = self.actor_client.clone();
            let registry_for_events = self.registry.clone();
            let config_for_events = self.config.clone();
            let event_handle = tokio::spawn(async move {
                while let Some(event) = event_rx.recv().await {
                    match event {
                        ActorDriverEvent::ReRegistrationRequired => {
                            tracing::warn!("Re-registration requested by server, re-registering actor handlers");
                            if let Some(ref actor_client) = actor_client_for_events {
                                let mut client = actor_client.lock().await;
                                let registry = registry_for_events.read().await;

                                // Re-register from the handler registry, which
                                // the #[operations] macro populates, the same way
                                // build() registers initially.
                                let mut handlers = Vec::new();
                                for (actor_name, operations) in registry.actor_handlers() {
                                    for (operation_name, registration) in operations {
                                        let mode = match registration.mode {
                                            crate::actor::types::OperationMode::Exclusive => {
                                                orcher_proto::orcher::v1::OperationMode::Exclusive
                                            }
                                            crate::actor::types::OperationMode::Shared => {
                                                orcher_proto::orcher::v1::OperationMode::Shared
                                            }
                                        };
                                        handlers.push(crate::actor::ActorHandlerInfo::new(
                                            actor_name.clone(),
                                            operation_name.clone(),
                                            mode,
                                        ));
                                    }
                                }

                                let mut metadata = std::collections::HashMap::new();
                                metadata.insert(
                                    "sdk_version".to_string(),
                                    env!("CARGO_PKG_VERSION").to_string(),
                                );
                                metadata.insert(
                                    "namespace".to_string(),
                                    config_for_events.namespace.clone(),
                                );

                                match client.register_handlers(handlers, metadata).await {
                                    Ok(response) if response.success => {
                                        tracing::info!(
                                            registration_id = %response.registration_id,
                                            "Successfully re-registered actor handlers"
                                        );
                                    }
                                    Ok(response) => {
                                        tracing::error!(
                                            error = %response.error_message,
                                            "Re-registration failed"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(error = %e, "Re-registration RPC failed");
                                    }
                                }
                            }
                        }
                    }
                }
            });

            (Some(driver_handle), Some(exec_handle), Some(event_handle))
        } else {
            (None, None, None)
        };

        // Worker registration driver: registers once, then heartbeats.
        let worker_reg_handle = {
            let registry = self.registry.read().await;
            #[cfg(feature = "auto-register")]
            let workflow_types: Vec<String> = registry
                .workflow_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            #[cfg(not(feature = "auto-register"))]
            let workflow_types: Vec<String> = Vec::new();

            #[cfg(feature = "auto-register")]
            let task_types: Vec<String> = registry
                .task_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            #[cfg(not(feature = "auto-register"))]
            let task_types: Vec<String> = Vec::new();

            #[cfg(not(feature = "auto-register"))]
            let _ = &registry;

            drop(registry);

            let mut metadata = std::collections::HashMap::new();
            metadata.insert("sdk".to_string(), "rust".to_string());
            metadata.insert(
                "sdk_version".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            );

            let mut reg_config = WorkerRegistrationConfig::default();
            reg_config.server_url = self.config.server_url.clone();
            reg_config.service_id = self.service_id.clone();
            reg_config.identity = self.config.identity.clone();
            reg_config.task_queue = self.config.task_queue.clone();
            reg_config.namespace = self.config.namespace.clone();
            reg_config.workflow_types = workflow_types;
            reg_config.task_types = task_types;
            reg_config.max_concurrent_workflows = self.config.max_concurrent_workflows as u32;
            reg_config.max_concurrent_tasks = self.config.max_concurrent_tasks as u32;
            reg_config.heartbeat_interval = self.config.heartbeat_interval;
            reg_config.metadata = metadata;
            reg_config.tls_config = None;
            reg_config.version_id = self.config.version_id.clone();
            // Registration must authenticate like polling does; otherwise a
            // server that requires credentials rejects it and the worker never
            // appears in the workers list.
            reg_config.api_key = self.config.api_key.clone();
            reg_config.organization_id = self.config.organization_id.clone();

            match WorkerRegistrationDriver::new(reg_config).await {
                Ok(mut driver) => {
                    tracing::info!("Worker registration driver started");
                    Some(tokio::spawn(async move {
                        if let Err(e) = driver.run().await {
                            tracing::error!(error = %e, "Worker registration driver error");
                        }
                    }))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to create worker registration driver — continuing without registration");
                    None
                }
            }
        };

        tracing::info!(
            identity = %self.service_id,
            "Worker started successfully"
        );

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Run until any driver or loop ends, or shutdown is requested.
        let (mut workflow_driver_stopped, mut task_driver_stopped) = (false, false);
        tokio::select! {
            _ = &mut workflow_driver_handle => {
                tracing::info!("Workflow driver completed");
                workflow_driver_stopped = true;
            }
            _ = &mut task_driver_handle => {
                tracing::info!("Task driver completed");
                task_driver_stopped = true;
            }
            _ = &mut workflow_exec_handle => {
                tracing::info!("Workflow execution loop completed");
            }
            _ = &mut task_exec_handle => {
                tracing::info!("Task execution loop completed");
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received");
            }
        }

        // Stop both drivers and wait for them while the execution loops keep
        // taking work and returning results. A driver left running, or dropped
        // mid-run, would abandon the results it had been handed, and each of
        // those activations or tasks would stay claimed on the server until its
        // timeout before another worker could run it. Signaling both first lets
        // their shutdown grace periods overlap.
        wf_stop.shutdown();
        task_stop.shutdown();
        let stop_workflow_driver = async {
            if !workflow_driver_stopped {
                stop_driver("Workflow", &mut workflow_driver_handle).await;
            }
        };
        let stop_task_driver = async {
            if !task_driver_stopped {
                stop_driver("Task", &mut task_driver_handle).await;
            }
        };
        tokio::join!(stop_workflow_driver, stop_task_driver);

        // Neither driver hands over more work, so the execution loops can stop.
        workflow_exec_handle.abort();
        task_exec_handle.abort();

        #[cfg(feature = "auto-register")]
        {
            if let Some(handle) = actor_driver_handle {
                tracing::debug!("Stopping actor driver");
                handle.abort();
            }
            if let Some(handle) = actor_exec_handle {
                tracing::debug!("Stopping actor execution loop");
                handle.abort();
            }
            if let Some(handle) = actor_event_handle {
                tracing::debug!("Stopping actor event listener");
                handle.abort();
            }
        }

        if let Some(handle) = worker_reg_handle {
            tracing::debug!("Stopping worker registration driver");
            handle.abort();
        }

        tracing::info!("Worker stopped");
        Ok(())
    }

    /// Copies the handlers collected in the registry into the runtime.
    async fn register_handlers_from_registry(&self) -> Result<()> {
        let registry = self.registry.read().await;

        #[cfg(feature = "auto-register")]
        {
            for name in registry.workflow_names() {
                if let Some(registration) = registry.get_workflow(name) {
                    if let Some(handler) = &registration.handler {
                        tracing::debug!(workflow_type = %name, "Registering workflow handler");
                        self.runtime
                            .register_workflow_handler(name.to_string(), handler.clone())
                            .await;
                    }
                }
            }

            for name in registry.task_names() {
                if let Some(registration) = registry.get_task(name) {
                    if let Some(handler) = &registration.handler {
                        tracing::debug!(task_type = %name, "Registering task handler");
                        self.runtime
                            .register_task_handler(name.to_string(), handler.clone())
                            .await;
                    }
                }
            }

            tracing::info!(
                workflows = registry.workflow_count(),
                tasks = registry.task_count(),
                "Handlers registered in execution runtime"
            );
        }

        #[cfg(not(feature = "auto-register"))]
        {
            tracing::debug!("auto-register feature not enabled, no handlers to register");
        }

        Ok(())
    }

    /// Executes one actor operation and returns its result bytes.
    ///
    /// The caller reports completion to the `ActorDriver` through the result
    /// channel. Without an executor, the operation is not run and a fixed
    /// placeholder result is returned.
    #[cfg(feature = "auto-register")]
    // The error is sdk-core's own, handed to its driver as it is; its size is
    // not this crate's to change.
    #[allow(clippy::result_large_err)]
    async fn execute_actor_op(
        actor_executor: Option<Arc<crate::actor::ActorExecutor>>,
        operation: &orcher_proto::orcher::v1::ActorOperation,
        server_url: &str,
    ) -> orcher_sdk_core::error::Result<Vec<u8>> {
        tracing::info!(
            actor = %operation.actor_name,
            key = %operation.key,
            operation = %operation.operation,
            operation_id = %operation.operation_id,
            "Executing actor operation"
        );

        let Some(executor) = actor_executor else {
            return Ok(serde_json::to_vec(&serde_json::json!({
                "status": "executed",
                "message": "Actor handler executed (placeholder)"
            }))
            .unwrap_or_default());
        };

        let actor_key =
            crate::actor::ActorKey::new(operation.actor_name.clone(), operation.key.clone());

        let state_client_config = crate::actor::ActorStateClientConfig {
            server_url: server_url.to_string(),
            ..Default::default()
        };

        let state_client = crate::actor::ActorStateClient::new(state_client_config)
            .await
            .map_err(|e| {
                orcher_sdk_core::error::Error::Internal(format!(
                    "Failed to create state client: {}",
                    e
                ))
            })?;

        let context = crate::actor::ActorContextBuilder::new(actor_key, Arc::new(state_client))
            .execution_id(&operation.execution_id)
            .build();

        executor
            .execute(
                &operation.actor_name,
                &operation.key,
                &operation.operation,
                operation.payload.clone(),
                context,
            )
            .await
            .map_err(|e| orcher_sdk_core::error::Error::Internal(e.to_string()))
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("server_url", &self.config.server_url)
            .field("namespace", &self.config.namespace)
            .field("task_queue", &self.config.task_queue)
            .field("identity", &self.config.identity)
            .field(
                "max_concurrent_workflows",
                &self.config.max_concurrent_workflows,
            )
            .field("max_concurrent_tasks", &self.config.max_concurrent_tasks)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert!(config.identity.starts_with("orcher-rust-"));
        assert_eq!(config.max_concurrent_workflows, 100);
        assert_eq!(config.max_concurrent_tasks, 200);
        assert_eq!(config.workflow_poller_count, 4);
        assert_eq!(config.task_poller_count, 4);
        assert_eq!(config.actor_poller_count, 4);
        assert_eq!(config.max_concurrent_actor_operations, 100);
    }
}
