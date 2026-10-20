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
//! The worker host (`orcher-sdk-host`, shared with the Python and TypeScript
//! SDKs) does the polling and all gRPC traffic, worker registration,
//! sessions, actor registration and shutdown; it hands work over by pull.
//! This crate runs the handlers, through [`ExecutionRuntime`]: workflow
//! activations as the kernel protocol carries them, tasks and actor
//! operations as host messages.

mod activation;
mod builder;
pub mod runtime;

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

use orcher_sdk_core::observability::WireMetricsSnapshot;
use orcher_sdk_core::proto::host::v1::{actor_result, ActorMode, ActorOperation, ActorResult};
use orcher_sdk_core::proto::kernel::v1::{run_activation_result, Payload as KernelPayload};
use orcher_sdk_host::{Host, HostConfig, Registrations};

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
/// A worker polls the orchestrator for work, executes the registered workflow,
/// task and actor handlers, and reports results back. The worker host does the
/// polling and gRPC; [`ExecutionRuntime`] stores and runs the handlers.
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

    /// Invokes actor operation handlers.
    #[cfg(feature = "auto-register")]
    actor_executor: Option<Arc<crate::actor::ActorExecutor>>,

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

    /// The worker host once [`run`](Self::run) has started it, kept after it
    /// stops so its wire measurements stay readable.
    host: std::sync::Mutex<Option<Host>>,
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

    /// TLS settings (default: None).
    ///
    /// `None` follows the URL scheme: `https://` connects over TLS verified
    /// against the system trust store, `http://` in plaintext. Set it to trust a
    /// private CA, present a client certificate (mTLS) or override the server
    /// name. It applies to every connection the worker opens: workflow, task
    /// and actor polling, and worker registration.
    pub tls: Option<crate::client::ClientTlsConfig>,

    /// Maximum number of concurrent worker sessions (default: 10).
    ///
    /// Sessions pin a series of tasks to a single worker. Each session
    /// occupies one slot. When all slots are in use, the worker that picked up
    /// a `create_session()` waits for one of its own slots to be released, up
    /// to the session's creation timeout; it does not pass the creation on to
    /// another worker. If none frees in time, `create_session()` returns
    /// `TaskFailed`.
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
            tls: None,
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
            .field("tls", &self.tls)
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

    /// What this worker's calls to the engine have done, by RPC method
    /// (calls, latency, outcomes, retries), and the work it has handed to its
    /// code and reported back. The same measurements every language SDK
    /// reports, to export as you like. `None` until [`run`](Self::run) has
    /// started the worker; still readable after it stops.
    pub fn wire_metrics(&self) -> Option<WireMetricsSnapshot> {
        self.host
            .lock()
            .ok()
            .and_then(|host| host.as_ref().map(Host::wire_metrics))
    }

    /// The TLS settings for sdk-core's drivers: the configured ones, or, for
    /// an `https://` address without any, TLS verified against the system
    /// trust store.
    fn core_tls(&self) -> Option<orcher_sdk_core::poller::TlsConfig> {
        crate::tls::tls_for_url(&self.config.server_url, self.config.tls.as_ref())
            .map(|tls| crate::tls::to_core(&tls))
    }

    /// The worker host's configuration, from this worker's.
    fn host_config(&self) -> HostConfig {
        let c = &self.config;
        let mut config = HostConfig::new(&c.server_url, &c.task_queue)
            .with_namespace(&c.namespace)
            .with_identity(&c.identity)
            .with_concurrency(
                c.max_concurrent_workflows as u32,
                c.max_concurrent_tasks as u32,
                c.max_concurrent_actor_operations as u32,
            )
            .with_pollers(
                c.workflow_poller_count,
                c.task_poller_count,
                c.actor_poller_count,
            )
            .with_heartbeat_interval(c.heartbeat_interval)
            .with_max_sessions(c.max_sessions)
            .with_sdk("rust", env!("CARGO_PKG_VERSION"))
            // A workflow failure is marked non-retryable only when the
            // workflow code raised it so (see `workflow_failure`).
            .with_protocol_version(orcher_sdk_core::worker_protocol::WORKFLOW_FAILURE_RETRYABILITY);
        if let Some(api_key) = &c.api_key {
            config = config.with_api_key(api_key);
        }
        if let Some(organization_id) = &c.organization_id {
            config = config.with_organization_id(organization_id);
        }
        if let Some(version_id) = &c.version_id {
            config = config.with_version_id(version_id);
        }
        if let Some(tls) = self.core_tls() {
            config = config.with_tls(tls);
        }
        config
    }

    /// What the worker runs: the workflows and tasks in its runtime, and the
    /// actor operations in its registry.
    async fn registrations(&self) -> Registrations {
        #[allow(unused_mut)]
        let mut registrations = Registrations::new(
            self.runtime.workflow_types().await,
            self.runtime.task_types().await,
            [] as [&str; 0],
        );
        #[cfg(feature = "auto-register")]
        {
            let registry = self.registry.read().await;
            let mut handlers = Vec::new();
            for (actor, operations) in registry.actor_handlers() {
                for (operation, registration) in operations {
                    handlers.push(match registration.mode {
                        crate::actor::types::OperationMode::Shared => {
                            orcher_sdk_host::ActorHandler::shared(actor, operation)
                        }
                        crate::actor::types::OperationMode::Exclusive => {
                            orcher_sdk_host::ActorHandler::exclusive(actor, operation)
                        }
                    });
                }
            }
            registrations = registrations.with_actor_handlers(handlers);
        }
        registrations
    }

    /// Runs the worker until shutdown is requested.
    ///
    /// The worker host (`orcher-sdk-host`) polls for work, reports results,
    /// registers the worker and its actor handlers, and runs sessions; this
    /// runs the handlers for the work it hands over, as many at a time as
    /// the worker is configured for. On [`shutdown()`](Self::shutdown) the
    /// host stops claiming work, gives the work running its shutdown grace,
    /// hands back workflow activations still unanswered, and deregisters the
    /// worker.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::StartupFailed`] if the worker cannot start:
    /// nothing is registered, the engine cannot be reached, or the actor
    /// service refuses the worker's actor handlers.
    ///
    /// [`WorkerError::StartupFailed`]: crate::error::WorkerError::StartupFailed
    pub async fn run(&self) -> Result<()> {
        // Listen before anything else: a shutdown asked for while the worker is
        // still starting would otherwise go unheard, and the worker would run on.
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!(
            identity = %self.config.identity,
            namespace = %self.config.namespace,
            task_queue = %self.config.task_queue,
            "Starting ORCHER Worker"
        );

        self.register_handlers_from_registry().await?;

        let host = tokio::select! {
            started = Host::start(self.host_config(), self.registrations().await) => {
                started.map_err(|e| {
                    Error::Worker(crate::error::WorkerError::StartupFailed(e.to_string()))
                })?
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown requested while the worker was starting");
                return Ok(());
            }
        };

        if let Ok(mut held) = self.host.lock() {
            *held = Some(host.clone());
        }

        let workflows = tokio::spawn(run_workflows(
            host.clone(),
            Arc::clone(&self.runtime),
            Arc::clone(&self.interceptor_chain),
            self.config.max_concurrent_workflows,
            self.config.namespace.clone(),
        ));
        let tasks = tokio::spawn(run_tasks(
            host.clone(),
            Arc::clone(&self.runtime),
            Arc::clone(&self.interceptor_chain),
            self.config.max_concurrent_tasks,
            self.config.namespace.clone(),
        ));
        #[cfg(feature = "auto-register")]
        let actors = self.actor_executor.clone().map(|executor| {
            tokio::spawn(run_actors(
                host.clone(),
                executor,
                self.config.max_concurrent_actor_operations,
            ))
        });

        tracing::info!(identity = %self.config.identity, "Worker started");
        let _ = shutdown_rx.recv().await;
        tracing::info!("Shutdown signal received");

        // Drains: the pulls end once the host has handed over what it had and
        // the work running has had its grace.
        host.shutdown().await;
        let _ = workflows.await;
        let _ = tasks.await;
        #[cfg(feature = "auto-register")]
        if let Some(actors) = actors {
            let _ = actors.await;
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
}

/// Runs the workflow activations the host hands over, at most `limit` at a
/// time: a pull is made only with room to run what it brings.
async fn run_workflows(
    host: Host,
    runtime: Arc<ExecutionRuntime>,
    interceptors: Arc<InterceptorChain>,
    limit: usize,
    namespace: String,
) {
    let room = Arc::new(tokio::sync::Semaphore::new(limit.max(1)));
    loop {
        let Ok(permit) = Arc::clone(&room).acquire_owned().await else {
            return;
        };
        let Some(activation) = host.next_activation().await else {
            return;
        };
        let (host, runtime, interceptors, namespace) = (
            host.clone(),
            Arc::clone(&runtime),
            Arc::clone(&interceptors),
            namespace.clone(),
        );
        tokio::spawn(async move {
            let run = activation.run.clone().unwrap_or_default();
            let ictx = orcher_sdk_core::interceptor::InterceptorContext::workflow(
                &run.workflow_id,
                &run.workflow_type,
                1,
                &namespace,
                &run.task_queue,
            );
            interceptors.before_execution(&ictx);
            let start = std::time::Instant::now();
            let answer = runtime.run_activation(activation).await;
            let duration_ms = start.elapsed().as_millis() as u64;
            match &answer.outcome {
                Some(run_activation_result::Outcome::Commands(_)) => {
                    interceptors.after_execution(&ictx, duration_ms)
                }
                Some(run_activation_result::Outcome::WorkflowFailed(failure)) => {
                    interceptors.on_error(&ictx, &failure.message, duration_ms)
                }
                Some(run_activation_result::Outcome::ProtocolError(error)) => {
                    interceptors.on_error(&ictx, &error.message, duration_ms)
                }
                None => interceptors.on_error(&ictx, "no outcome", duration_ms),
            }
            if let Err(e) = host.complete_activation(answer).await {
                tracing::error!(error = %e, "Failed to hand the workflow activation's answer back");
            }
            drop(permit);
        });
    }
}

/// Runs the tasks the host hands over, at most `limit` at a time.
async fn run_tasks(
    host: Host,
    runtime: Arc<ExecutionRuntime>,
    interceptors: Arc<InterceptorChain>,
    limit: usize,
    namespace: String,
) {
    let room = Arc::new(tokio::sync::Semaphore::new(limit.max(1)));
    loop {
        let Ok(permit) = Arc::clone(&room).acquire_owned().await else {
            return;
        };
        let Some(assignment) = host.next_task().await else {
            return;
        };
        let (host, runtime, interceptors, namespace) = (
            host.clone(),
            Arc::clone(&runtime),
            Arc::clone(&interceptors),
            namespace.clone(),
        );
        tokio::spawn(async move {
            let task_queue = assignment
                .run
                .as_ref()
                .map(|run| run.task_queue.clone())
                .unwrap_or_default();
            let ictx = orcher_sdk_core::interceptor::InterceptorContext::task(
                &assignment.task_id,
                &assignment.task_type,
                assignment.attempt.max(1),
                &namespace,
                &task_queue,
            );
            interceptors.before_execution(&ictx);
            let start = std::time::Instant::now();
            let outcome = runtime.execute_assignment(assignment, &host).await;
            let duration_ms = start.elapsed().as_millis() as u64;
            match &outcome.result {
                Some(orcher_sdk_core::proto::host::v1::task_outcome::Result::Failure(failure)) => {
                    interceptors.on_error(&ictx, &failure.message, duration_ms)
                }
                _ => interceptors.after_execution(&ictx, duration_ms),
            }
            if let Err(e) = host.complete_task(outcome).await {
                tracing::error!(error = %e, "Failed to report the task's outcome");
            }
            drop(permit);
        });
    }
}

/// Runs the actor operations the host hands over, at most `limit` at a time.
/// State is read and written through the host.
#[cfg(feature = "auto-register")]
async fn run_actors(host: Host, executor: Arc<crate::actor::ActorExecutor>, limit: usize) {
    let room = Arc::new(tokio::sync::Semaphore::new(limit.max(1)));
    loop {
        let Ok(permit) = Arc::clone(&room).acquire_owned().await else {
            return;
        };
        let Some(operation) = host.next_actor_operation().await else {
            return;
        };
        let (host, executor) = (host.clone(), Arc::clone(&executor));
        tokio::spawn(async move {
            let result = run_actor_operation(&host, &executor, &operation).await;
            if let Err(e) = host
                .complete_actor_operation(ActorResult {
                    operation_id: operation.operation_id,
                    execution_id: operation.execution_id,
                    result: Some(result),
                    ..Default::default()
                })
                .await
            {
                tracing::error!(error = %e, "Failed to report the actor operation's result");
            }
            drop(permit);
        });
    }
}

#[cfg(feature = "auto-register")]
async fn run_actor_operation(
    host: &Host,
    executor: &crate::actor::ActorExecutor,
    operation: &ActorOperation,
) -> actor_result::Result {
    tracing::info!(
        actor = %operation.actor_name,
        key = %operation.key,
        operation = %operation.operation,
        operation_id = %operation.operation_id,
        shared = operation.mode == ActorMode::Shared as i32,
        "Executing actor operation"
    );
    let state = Arc::new(crate::actor::ActorStateClient::from_host(host.clone()));
    let context = crate::actor::ActorContextBuilder::new(
        crate::actor::ActorKey::new(operation.actor_name.clone(), operation.key.clone()),
        state,
    )
    .execution_id(&operation.execution_id)
    .build();
    let payload = operation
        .payload
        .as_ref()
        .map(|payload| payload.data.clone())
        .unwrap_or_default();
    match executor
        .execute(
            &operation.actor_name,
            &operation.key,
            &operation.operation,
            payload,
            context,
        )
        .await
    {
        Ok(data) => actor_result::Result::Value(KernelPayload {
            data,
            ..Default::default()
        }),
        Err(error) => actor_result::Result::Failure(runtime::failure_of(error)),
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
