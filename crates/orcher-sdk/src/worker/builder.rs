//! Builder for configuring and creating a [`Worker`].

use super::runtime::{ExecutionRuntime, RuntimeConfig};
use super::Registry;
use super::{InterceptorChain, Worker, WorkerConfig};
use crate::error::{Error, Result};
use crate::interceptor::InterceptorHook;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Configures and builds a [`Worker`].
///
/// With the `auto-register` feature, handlers declared with the SDK's macros
/// are collected when the builder is created.
///
/// ## Example
///
/// ```rust,no_run
/// # use orcher_sdk::prelude::*;
/// # async fn example() -> Result<()> {
/// let service = WorkerBuilder::new()
///     .server_url("http://localhost:50051")
///     .namespace("production")
///     .task_queue("orders")
///     .identity("order-service-1")
///     .max_concurrent_workflows(50)
///     .max_concurrent_tasks(100)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct WorkerBuilder {
    config: WorkerConfig,
    registry: Registry,
    interceptor_chain: InterceptorChain,
}

impl WorkerBuilder {
    /// Creates a builder with the default configuration.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new();
    /// ```
    pub fn new() -> Self {
        let config = WorkerConfig::default();

        let mut registry = Registry::new();

        #[cfg(feature = "auto-register")]
        {
            registry.collect_from_global_registry();

            tracing::debug!(
                tasks = registry.task_count(),
                workflows = registry.workflow_count(),
                actors = registry.actor_count(),
                "Auto-registered handlers from global registry"
            );
        }

        Self {
            config,
            registry,
            interceptor_chain: InterceptorChain::new(),
        }
    }

    /// Sets the server URL.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .server_url("http://localhost:50051");
    /// ```
    pub fn server_url(mut self, url: impl Into<String>) -> Self {
        self.config.server_url = url.into();
        self
    }

    /// Sets the namespace.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .namespace("production");
    /// ```
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.config.namespace = namespace.into();
        self
    }

    /// Sets the task queue to poll.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .task_queue("orders");
    /// ```
    pub fn task_queue(mut self, queue: impl Into<String>) -> Self {
        self.config.task_queue = queue.into();
        self
    }

    /// Sets the identity reported to the server, for observability.
    ///
    /// Defaults to `orcher-rust-` followed by a random UUID.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .identity("order-service-1");
    /// ```
    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.config.identity = identity.into();
        self
    }

    /// Sets how often the worker tells the server it is alive (default: 10 seconds).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::time::Duration;
    /// let builder = WorkerBuilder::new()
    ///     .heartbeat_interval(Duration::from_secs(5));
    /// ```
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Sets the maximum number of concurrent workflow executions (default: 100).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .max_concurrent_workflows(50);
    /// ```
    pub fn max_concurrent_workflows(mut self, max: usize) -> Self {
        self.config.max_concurrent_workflows = max;
        self
    }

    /// Sets the maximum number of concurrent task executions (default: 200).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .max_concurrent_tasks(100);
    /// ```
    pub fn max_concurrent_tasks(mut self, max: usize) -> Self {
        self.config.max_concurrent_tasks = max;
        self
    }

    /// Sets the interval between polls for work (default: 100 ms).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::time::Duration;
    /// let builder = WorkerBuilder::new()
    ///     .poll_interval(Duration::from_millis(200));
    /// ```
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    /// Sets the number of concurrent workflow pollers (default: 4, minimum: 1).
    ///
    /// Each poller holds its own long-poll request, so more pollers raise
    /// throughput when many workflows run at once.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .workflow_poller_count(8);
    /// ```
    pub fn workflow_poller_count(mut self, count: usize) -> Self {
        self.config.workflow_poller_count = count.max(1);
        self
    }

    /// Sets the number of concurrent task pollers (default: 4, minimum: 1).
    ///
    /// Each poller holds its own long-poll request, so more pollers raise
    /// throughput when many tasks run at once.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .task_poller_count(8);
    /// ```
    pub fn task_poller_count(mut self, count: usize) -> Self {
        self.config.task_poller_count = count.max(1);
        self
    }

    /// Sets the number of concurrent actor operation pollers (default: 4, minimum: 1).
    ///
    /// Used only when actors are registered.
    pub fn actor_poller_count(mut self, count: usize) -> Self {
        self.config.actor_poller_count = count.max(1);
        self
    }

    /// Sets the maximum number of concurrent actor operations (default: 100).
    pub fn max_concurrent_actor_operations(mut self, max: usize) -> Self {
        self.config.max_concurrent_actor_operations = max;
        self
    }

    /// Sets the maximum number of concurrent worker sessions (default: 10).
    ///
    /// Sessions pin a series of tasks to a single worker. Each session
    /// occupies one slot; when all slots are in use, `create_session()` calls
    /// wait until a slot is released or the creation timeout expires.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .max_sessions(20);
    /// ```
    pub fn max_sessions(mut self, max: usize) -> Self {
        self.config.max_sessions = max;
        self
    }

    /// Sets the organization to act for.
    ///
    /// Requests carry it in the `X-Organization-Id` header, which the server
    /// uses for organization-level quotas and billing attribution.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .organization_id("org_abc123");
    /// ```
    pub fn organization_id(mut self, org_id: impl Into<String>) -> Self {
        self.config.organization_id = Some(org_id.into());
        self
    }

    /// Sets the API key that authenticates the worker with the server.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.config.api_key = Some(key.into());
        self
    }

    /// Sets the TLS settings for every connection the worker opens.
    ///
    /// An `https://` server URL needs none of this: it connects over TLS
    /// verified against the system trust store by default. Supply settings to
    /// trust a private CA, present a client certificate (mTLS) or override the
    /// server name. They take the same [`ClientTlsConfig`] as the client.
    /// TLS is only negotiated for `https://` addresses.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher_sdk::prelude::*;
    /// # use orcher_sdk::client::ClientTlsConfig;
    /// # async fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let worker = WorkerBuilder::new()
    ///     .server_url("https://orcher.internal:443")
    ///     .task_queue("orders")
    ///     .tls(
    ///         ClientTlsConfig::new()
    ///             .with_ca_cert_file("ca.pem")?
    ///             .with_client_identity_files("client.pem", "client-key.pem")?,
    ///     )
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`ClientTlsConfig`]: crate::client::ClientTlsConfig
    pub fn tls(mut self, tls: crate::client::ClientTlsConfig) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Declares the code release this worker is running.
    ///
    /// An opaque label, such as a git SHA, an image digest or a release tag. The
    /// server records it on a workflow execution the first time a worker claims
    /// it, so that the execution keeps running against the code it started on.
    /// The label is never parsed or ordered. A blank value clears it.
    ///
    /// Defaults to the `ORCHER_VERSION_ID` environment variable, since the value
    /// usually comes from the build. Leaving it unset declares nothing and
    /// changes no behavior.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// let builder = WorkerBuilder::new()
    ///     .version_id("v1.2.3-abc123");
    /// ```
    pub fn version_id(mut self, id: impl Into<String>) -> Self {
        let v = id.into();
        self.config.version_id = if v.trim().is_empty() { None } else { Some(v) };
        self
    }

    /// Adds an interceptor that runs around every workflow and task execution.
    ///
    /// Interceptors are called in the order they are added. Use the built-in
    /// ones or implement [`InterceptorHook`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use orcher_sdk::interceptor::{LoggingInterceptor, MetricsInterceptor};
    /// let builder = WorkerBuilder::new()
    ///     .interceptor(LoggingInterceptor::new())
    ///     .interceptor(MetricsInterceptor::new());
    /// ```
    pub fn interceptor(mut self, interceptor: impl InterceptorHook + 'static) -> Self {
        self.interceptor_chain.add(interceptor);
        self
    }

    /// Decrypts incoming payloads with AES-256-GCM.
    ///
    /// The key must match the one used by the client that started the workflow.
    /// Without this, encrypted payloads reach handlers as opaque bytes. Replaces
    /// any codec chain already configured.
    pub fn with_encryption(mut self, key: &[u8; 32]) -> Self {
        use orcher_sdk_core::codec::{CodecChain, EncryptionPayloadCodec};
        let chain = CodecChain::new().with_codec(EncryptionPayloadCodec::new(key));
        self.config.codec_chain = Some(Arc::new(chain));
        self
    }

    /// Decompresses incoming gzip payloads.
    ///
    /// Replaces any codec chain already configured.
    pub fn with_gzip_compression(mut self) -> Self {
        use orcher_sdk_core::codec::{CodecChain, GzipPayloadCodec};
        let chain = CodecChain::new().with_codec(GzipPayloadCodec::default());
        self.config.codec_chain = Some(Arc::new(chain));
        self
    }

    /// Builds the [`Worker`].
    ///
    /// When actor handlers are registered, they are registered with the server
    /// here; nothing else contacts the server until [`Worker::run`].
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::InvalidConfiguration`] if the server URL or task
    /// queue is empty. Fails if actor handlers are registered and their
    /// registration with the server fails.
    ///
    /// [`WorkerError::InvalidConfiguration`]: crate::error::WorkerError::InvalidConfiguration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher_sdk::prelude::*;
    /// # async fn example() -> Result<()> {
    /// let service = WorkerBuilder::new()
    ///     .server_url("http://localhost:50051")
    ///     .task_queue("orders")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> Result<Worker> {
        if self.config.server_url.is_empty() {
            return Err(Error::Worker(
                crate::error::WorkerError::InvalidConfiguration(
                    "server_url cannot be empty".to_string(),
                ),
            ));
        }

        if self.config.task_queue.is_empty() {
            return Err(Error::Worker(
                crate::error::WorkerError::InvalidConfiguration(
                    "task_queue cannot be empty".to_string(),
                ),
            ));
        }

        // Reject unusable TLS settings now rather than at the first poll:
        // a certificate without its key, or PEM that does not parse.
        if self.config.tls.is_some() {
            crate::tls::endpoint(&self.config.server_url, self.config.tls.as_ref()).map_err(
                |reason| Error::Worker(crate::error::WorkerError::InvalidConfiguration(reason)),
            )?;
        }

        tracing::info!(
            server_url = %self.config.server_url,
            namespace = %self.config.namespace,
            task_queue = %self.config.task_queue,
            identity = %self.config.identity,
            tasks = self.registry.task_count(),
            workflows = self.registry.workflow_count(),
            actors = {
                #[cfg(feature = "auto-register")]
                { self.registry.actor_count() }
                #[cfg(not(feature = "auto-register"))]
                { 0 }
            },
            "Building ORCHER Worker (driver mode)"
        );

        let runtime_config = RuntimeConfig {
            max_concurrent_workflows: self.config.max_concurrent_workflows,
            max_concurrent_tasks: self.config.max_concurrent_tasks,
            default_task_queue: self.config.task_queue.clone(),
            namespace: self.config.namespace.clone(),
            codec_chain: self.config.codec_chain.clone(),
        };
        let runtime = Arc::new(ExecutionRuntime::new(runtime_config));

        // Register actors with the server.
        //
        // Both the gate and the handler list must come from the handler
        // registry, which the `#[operations]` macro populates
        // (`register_actor_handler_direct`). The definitions registry
        // (`actor_count`/`get_actor`) is filled only by the manual definition
        // APIs; gating on it would leave macro-registered actors with no server
        // registration and no actor driver.
        #[cfg(feature = "auto-register")]
        let actor_client = {
            if self.registry.actor_handler_count() > 0 {
                tracing::info!(
                    handlers = self.registry.actor_handler_count(),
                    "Registering actor handlers with server"
                );

                let mut client = crate::actor::ActorClient::connect_with_tls(
                    self.config.server_url.clone(),
                    self.config.identity.clone(),
                    self.config.tls.clone(),
                )
                .await?;

                let mut handlers = Vec::new();
                for (actor_name, operations) in self.registry.actor_handlers() {
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
                metadata.insert("namespace".to_string(), self.config.namespace.clone());
                metadata.insert("task_queue".to_string(), self.config.task_queue.clone());

                let response = client.register_handlers(handlers, metadata).await?;

                if !response.success {
                    return Err(Error::Worker(crate::error::WorkerError::StartupFailed(
                        format!("Failed to register actors: {}", response.error_message),
                    )));
                }

                tracing::info!(
                    registration_id = %response.registration_id,
                    handlers_registered = response.handlers_registered,
                    "Successfully registered actors with server"
                );

                let registration_id = response.registration_id.clone();

                Some((Arc::new(tokio::sync::Mutex::new(client)), registration_id))
            } else {
                None
            }
        };

        #[cfg(feature = "auto-register")]
        let actor_executor = {
            if self.registry.actor_handler_count() > 0 {
                let executor = crate::actor::ActorExecutor::new();

                for (actor_name, operations) in self.registry.actor_handlers() {
                    for (operation_name, handler_registration) in operations {
                        executor.register_handler(
                            actor_name,
                            operation_name,
                            handler_registration.handler.clone(),
                        );
                    }
                }

                tracing::info!(
                    handlers = executor.handler_count(),
                    "Actor executor initialized"
                );

                Some(Arc::new(executor))
            } else {
                None
            }
        };

        tracing::info!(
            identity = %self.config.identity,
            "Worker built successfully"
        );

        #[cfg(feature = "auto-register")]
        let (actor_client, actor_registration_id) = match actor_client {
            Some((client, reg_id)) => (Some(client), Some(reg_id)),
            None => (None, None),
        };

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        if !self.interceptor_chain.is_empty() {
            tracing::info!(
                interceptors = ?self.interceptor_chain,
                "Interceptor chain configured"
            );
        }

        Ok(Worker {
            config: self.config.clone(),
            runtime,
            registry: Arc::new(RwLock::new(self.registry)),
            #[cfg(feature = "auto-register")]
            actor_client,
            #[cfg(feature = "auto-register")]
            actor_executor,
            #[cfg(feature = "auto-register")]
            actor_registration_id,
            service_id: self.config.identity.clone(),
            shutdown_tx,
            started_at: std::time::Instant::now(),
            interceptor_chain: Arc::new(self.interceptor_chain),
        })
    }
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_configuration() {
        let builder = WorkerBuilder::new()
            .server_url("http://test:1234")
            .namespace("test-ns")
            .task_queue("test-queue")
            .identity("test-service")
            .max_concurrent_workflows(50)
            .max_concurrent_tasks(100)
            .poll_interval(Duration::from_millis(200));

        assert_eq!(builder.config.server_url, "http://test:1234");
        assert_eq!(builder.config.namespace, "test-ns");
        assert_eq!(builder.config.task_queue, "test-queue");
        assert_eq!(builder.config.identity, "test-service");
        assert_eq!(builder.config.max_concurrent_workflows, 50);
        assert_eq!(builder.config.max_concurrent_tasks, 100);
        assert_eq!(builder.config.poll_interval, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn tls_settings_reach_the_driver_configs() {
        let worker = WorkerBuilder::new()
            .server_url("https://10.0.0.1:443")
            .task_queue("q")
            .tls(crate::client::ClientTlsConfig::new().with_domain_name("orcher.internal"))
            .build()
            .await
            .expect("builds");
        let core = worker.core_tls().expect("TLS configured");
        assert_eq!(core.domain_name.as_deref(), Some("orcher.internal"));
    }

    #[tokio::test]
    async fn pem_that_does_not_parse_fails_the_build() {
        let result = WorkerBuilder::new()
            .server_url("https://orcher.example:443")
            .task_queue("q")
            .tls(
                crate::client::ClientTlsConfig::new()
                    .with_client_identity(b"not a cert".to_vec(), b"not a key".to_vec()),
            )
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::Worker(
                crate::error::WorkerError::InvalidConfiguration(_)
            ))
        ));
    }

    #[tokio::test]
    async fn an_https_url_gets_tls_without_settings_and_http_does_not() {
        let https = WorkerBuilder::new()
            .server_url("https://orcher.example:443")
            .task_queue("q")
            .build()
            .await
            .unwrap();
        let core = https.core_tls().expect("https gets TLS");
        assert!(core.ca_cert.is_none(), "system trust store");

        let http = WorkerBuilder::new()
            .server_url("http://localhost:50051")
            .task_queue("q")
            .build()
            .await
            .unwrap();
        assert!(http.core_tls().is_none());
    }

    #[tokio::test]
    async fn a_client_certificate_without_a_key_fails_the_build() {
        let mut tls = crate::client::ClientTlsConfig::new();
        tls.client_cert = Some(b"cert".to_vec());
        let result = WorkerBuilder::new()
            .server_url("https://orcher.example:443")
            .task_queue("q")
            .tls(tls)
            .build()
            .await;
        match result {
            Err(Error::Worker(crate::error::WorkerError::InvalidConfiguration(reason))) => {
                assert!(reason.contains("must be set together"), "{reason}")
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("must fail"),
        }
    }

    #[test]
    fn tls_files_that_do_not_exist_name_the_path() {
        let err = crate::client::ClientTlsConfig::new()
            .with_ca_cert_file("/nonexistent/ca.pem")
            .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");
    }

    #[tokio::test]
    async fn test_builder_validation_empty_url() {
        let result = WorkerBuilder::new()
            .server_url("")
            .task_queue("test-queue")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Worker(_)));
    }

    #[tokio::test]
    async fn test_builder_validation_empty_queue() {
        let result = WorkerBuilder::new()
            .server_url("http://localhost:50051")
            .task_queue("")
            .build()
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Worker(_)));
    }

    #[test]
    fn test_builder_default() {
        let builder = WorkerBuilder::default();
        assert_eq!(builder.config.server_url, "http://localhost:50051");
    }
}
