//! High-level client for the ORCHER orchestrator.

use crate::actor::{ActorInvocationClient, ActorRef};
use crate::error::{Error, Result};
use orcher_sdk_core::client::{CoreActorClient, NamespaceClient, WorkflowClient as CoreClient};
use orcher_sdk_core::codec::PayloadCodec;
use orcher_sdk_core::converter::{BinaryPayloadConverter, DataConverter, JsonPayloadConverter};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use super::{ClientConfig, StartWorkflowOptions, WorkflowHandle};

/// Converts a std `Duration` to the protobuf `Duration` used in workflow requests.
fn proto_duration(d: std::time::Duration) -> orcher_proto::prost_types::Duration {
    orcher_proto::prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// Converts an SDK `RetryPolicy` to the protobuf form sent with a start request.
fn proto_retry_policy(
    p: &crate::task::RetryPolicy,
) -> orcher_sdk_core::proto::orcher::v1::RetryPolicy {
    orcher_sdk_core::proto::orcher::v1::RetryPolicy {
        initial_interval: Some(proto_duration(p.initial_interval)),
        backoff_coefficient: p.backoff_coefficient,
        maximum_interval: Some(proto_duration(p.max_interval)),
        maximum_attempts: p.max_attempts as i32,
        non_retryable_error_types: Vec::new(),
        ..Default::default()
    }
}

/// Data converter selected by the client.
///
/// An enum rather than a trait object because `DataConverter` has generic
/// methods and so is not object-safe.
#[derive(Clone)]
pub enum DataConverterType {
    /// Encode values as JSON.
    Json(JsonPayloadConverter),
    /// Encode values in the binary payload format.
    Binary(BinaryPayloadConverter),
}

impl DataConverterType {
    /// Encode `value` with this converter.
    // The error type comes from sdk-core. Boxing it would change a public
    // signature for no benefit on a path that is not hot.
    #[allow(clippy::result_large_err)]
    pub fn to_payload<T: Serialize>(
        &self,
        value: &T,
    ) -> orcher_sdk_core::error::Result<orcher_sdk_core::payload::Payload> {
        match self {
            DataConverterType::Json(converter) => converter.to_payload(value),
            DataConverterType::Binary(converter) => converter.to_payload(value),
        }
    }

    /// Name of the encoding this converter produces, such as `json`.
    pub fn encoding(&self) -> &str {
        match self {
            DataConverterType::Json(converter) => converter.encoding(),
            DataConverterType::Binary(converter) => converter.encoding(),
        }
    }
}

impl Default for DataConverterType {
    fn default() -> Self {
        DataConverterType::Json(JsonPayloadConverter)
    }
}

/// Connection to the ORCHER orchestrator for starting and managing workflows.
///
/// Cloning is cheap: clones share the underlying gRPC channel.
///
/// ## Example
///
/// ```rust,no_run
/// # use orcher_sdk::prelude::*;
/// # async fn example() -> Result<()> {
/// let client = Client::connect("http://localhost:50051").await?;
/// let handle = client.start_workflow("MyWorkflow", "input-data").await?;
/// let result: String = handle.result().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    pub(crate) core_client: Arc<CoreClient>,
    pub(crate) actor_client: Arc<CoreActorClient>,
    pub(crate) namespace_client: Arc<NamespaceClient>,
    pub(crate) config: ClientConfig,
    pub(crate) data_converter: DataConverterType,
    /// The connection every gRPC client above shares, kept so the clients can
    /// be rebuilt with new credentials.
    channel: tonic::transport::Channel,
}

impl Client {
    /// Connects to the ORCHER server at `url` with the default configuration.
    ///
    /// The URL looks like `http://localhost:50051`. An `https://` URL connects
    /// over TLS verified against the system trust store; for a private CA or
    /// mTLS, use [`Client::with_config`] with [`ClientConfig::with_tls`]. The
    /// channel connects lazily, so an unreachable server surfaces on the first
    /// call, not here.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ConnectionFailed`] if the URL is invalid.
    ///
    /// [`ClientError::ConnectionFailed`]: crate::error::ClientError::ConnectionFailed
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let config = ClientConfig {
            server_url: url.clone(),
            ..Default::default()
        };

        Self::with_config(config).await
    }

    /// Creates a client from a full [`ClientConfig`].
    ///
    /// The API key and organization ID are bound to every gRPC client here.
    /// TLS follows the URL scheme (`https://` enables it) unless
    /// [`ClientConfig::tls`] is set, which is how a private CA or a client
    /// certificate is supplied.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ConnectionFailed`] if the URL is invalid or the TLS
    /// settings are rejected.
    ///
    /// [`ClientError::ConnectionFailed`]: crate::error::ClientError::ConnectionFailed
    pub async fn with_config(config: ClientConfig) -> Result<Self> {
        tracing::info!(
            server_url = %config.server_url,
            namespace = %config.namespace,
            tls = crate::tls::tls_for_url(&config.server_url, config.tls.as_ref()).is_some(),
            "Connecting to ORCHER server via gRPC"
        );

        // TLS follows the URL scheme unless set explicitly; see `tls_for_url`.
        let endpoint =
            crate::tls::endpoint(&config.server_url, config.tls.as_ref()).map_err(|reason| {
                Error::Client(crate::error::ClientError::ConnectionFailed {
                    url: config.server_url.clone(),
                    reason,
                })
            })?;

        // One channel shared by the workflow, actor and namespace clients.
        let channel = endpoint
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(config.timeout)
            .connect_lazy();

        Ok(Self::from_channel(config, channel))
    }

    /// Builds the gRPC clients on `channel` with the credentials in `config`.
    fn from_channel(config: ClientConfig, channel: tonic::transport::Channel) -> Self {
        // Credentials are attached by an interceptor bound here, so every RPC
        // carries them.
        let mut core_client = CoreClient::from_channel(channel.clone())
            .with_namespace(&config.namespace)
            .with_timeout(config.timeout);
        if let Some(ref key) = config.api_key {
            core_client = core_client.with_api_key(key.clone());
        }
        if let Some(ref organization_id) = config.organization_id {
            core_client = core_client.with_organization_id(organization_id.clone());
        }

        // The actor and namespace clients must share the workflow client's
        // credentials. Built from the bare channel, their calls would go out
        // unauthenticated against a server that requires an API key.
        let credentials = core_client.auth().clone();
        let actor_client = Arc::new(CoreActorClient::with_auth(
            channel.clone(),
            credentials.clone(),
        ));
        let namespace_client = Arc::new(NamespaceClient::with_auth(channel.clone(), credentials));

        let data_converter = config.data_converter.clone().unwrap_or_default();

        Self {
            core_client: Arc::new(core_client),
            actor_client,
            namespace_client,
            config,
            data_converter,
            channel,
        }
    }

    // ============================================================================
    // Configuration
    // ============================================================================

    /// Compresses workflow inputs and outputs with gzip.
    ///
    /// Replaces any codec chain already configured.
    pub fn with_gzip_compression(mut self) -> Self {
        use orcher_sdk_core::codec::{CodecChain, GzipPayloadCodec};

        let chain = CodecChain::new().with_codec(GzipPayloadCodec::default());

        self.config.codec_chain = Some(Arc::new(chain));
        self
    }

    /// Encrypts workflow inputs and outputs with AES-256-GCM.
    ///
    /// Replaces any codec chain already configured. The server decrypts
    /// payloads and stores plain JSON, so this protects data in transit only;
    /// it is not end-to-end encryption.
    pub fn with_encryption(mut self, key: &[u8; 32]) -> Self {
        use orcher_sdk_core::codec::{CodecChain, EncryptionPayloadCodec};

        let chain = CodecChain::new().with_codec(EncryptionPayloadCodec::new(key));

        self.config.codec_chain = Some(Arc::new(chain));
        self
    }

    /// Has no effect: TLS belongs to the connection, which is already built.
    ///
    /// Set TLS with [`ClientConfig::with_tls`] before
    /// [`Client::with_config`]. An `https://` URL needs no TLS settings at all.
    #[deprecated(
        note = "has no effect on a connected client; set TLS with ClientConfig::with_tls before Client::with_config (https:// URLs use TLS automatically)"
    )]
    pub fn with_tls(mut self, tls: super::ClientTlsConfig) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Authenticates every later call with this API key.
    ///
    /// The workflow, actor and namespace clients are rebuilt on the existing
    /// connection, so the key is sent from the next call on. Clones taken
    /// before this call keep their old credentials.
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.config.api_key = Some(key.into());
        let channel = self.channel.clone();
        let data_converter = self.data_converter.clone();
        let mut rebuilt = Self::from_channel(self.config, channel);
        // Keep a converter chosen after connecting (use_binary_serialization).
        rebuilt.data_converter = data_converter;
        rebuilt
    }

    /// Serializes values in the binary format instead of JSON.
    ///
    /// Binary is more compact but not human-readable.
    pub fn use_binary_serialization(mut self) -> Self {
        self.data_converter = DataConverterType::Binary(BinaryPayloadConverter);
        self
    }

    // ============================================================================
    // Starting Workflows
    // ============================================================================

    /// Starts a workflow with a generated ID on the `default` task queue.
    ///
    /// # Errors
    ///
    /// Fails if the input cannot be serialized or the server rejects the start.
    pub async fn start_workflow<I>(
        &self,
        workflow_type: impl Into<String>,
        input: I,
    ) -> Result<WorkflowHandle>
    where
        I: Serialize,
    {
        let workflow_id = uuid::Uuid::new_v4().to_string();
        let options = StartWorkflowOptions {
            workflow_id: Some(workflow_id.clone()),
            task_queue: "default".to_string(),
            ..Default::default()
        };

        self.start_workflow_with_options(workflow_type, input, options)
            .await
    }

    /// Starts a workflow with explicit options.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::WorkflowAlreadyExists`] if the workflow ID is held
    /// by an open run and the reuse policy does not allow replacing it. Fails
    /// with [`ClientError::RequestFailed`] if the input cannot be serialized or
    /// encoded, or the server rejects the start.
    ///
    /// [`ClientError::WorkflowAlreadyExists`]: crate::error::ClientError::WorkflowAlreadyExists
    /// [`ClientError::RequestFailed`]: crate::error::ClientError::RequestFailed
    pub async fn start_workflow_with_options<I>(
        &self,
        workflow_type: impl Into<String>,
        input: I,
        options: StartWorkflowOptions,
    ) -> Result<WorkflowHandle>
    where
        I: Serialize,
    {
        let workflow_type = workflow_type.into();
        let workflow_id = options
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let namespace = options
            .namespace
            .as_ref()
            .unwrap_or(&self.config.namespace)
            .clone();

        tracing::info!(
            workflow_type = %workflow_type,
            workflow_id = %workflow_id,
            task_queue = %options.task_queue,
            namespace = %namespace,
            "Starting workflow"
        );

        let client = self.core_client.as_ref().clone().with_namespace(&namespace);

        let input_data = if self.config.use_payload_format {
            let mut payload = self.data_converter.to_payload(&input).map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to serialize input to Payload: {}",
                    e
                )))
            })?;

            if let Some(codec_chain) = &self.config.codec_chain {
                payload = codec_chain.encode(&payload).map_err(|e| {
                    Error::Client(crate::error::ClientError::RequestFailed(format!(
                        "Failed to encode Payload with codec chain: {}",
                        e
                    )))
                })?;
            }

            serde_json::to_vec(&payload).map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to serialize Payload envelope: {}",
                    e
                )))
            })?
        } else {
            // Plain JSON, without an envelope.
            serde_json::to_vec(&input).map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to serialize input: {}",
                    e
                )))
            })?
        };

        // Build the full request so that every option (retry policy, timeouts,
        // reuse policy) reaches the server; `start_workflow_raw` would drop them.
        // Submit through the single-start call, because the batch endpoint
        // re-wraps the input.
        let request = orcher_sdk_core::proto::orcher::v1::StartWorkflowRequest {
            workflow_id: workflow_id.clone(),
            workflow_type,
            task_queue: options.task_queue.clone(),
            namespace: namespace.clone(),
            input: input_data,
            execution_timeout: options.workflow_execution_timeout.map(proto_duration),
            run_timeout: options.workflow_run_timeout.map(proto_duration),
            task_timeout: options.workflow_task_timeout.map(proto_duration),
            retry_policy: options.retry_policy.as_ref().map(proto_retry_policy),
            cron_schedule: options.cron_schedule.clone().unwrap_or_default(),
            annotations: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            request_id: uuid::Uuid::new_v4().to_string(),
            start_delay: None,
            schedule_config: None,
            workflow_id_reuse_policy: options
                .id_reuse_policy
                .map_or(0, |policy| policy.to_proto() as i32),
            ..Default::default()
        };
        let handle = client
            .start_workflow_request(request)
            .await
            .map_err(|e| match e {
                // Surface an id conflict as its own error, naming the open run,
                // rather than folding it into a generic failure.
                already @ orcher_sdk_core::error::Error::WorkflowAlreadyExists { .. } => {
                    Error::from(already)
                }
                e => Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to start workflow: {}",
                    e
                ))),
            })?;

        tracing::debug!(
            workflow_id = %handle.workflow_id(),
            run_id = %handle.run_id(),
            "Workflow started successfully"
        );

        // Keep the namespace on the handle so later calls target the same one.
        Ok(WorkflowHandle::new_with_namespace(
            Arc::clone(&self.core_client),
            handle.workflow_id().to_string(),
            Some(handle.run_id().to_string()),
            Some(namespace),
        ))
    }

    /// Starts several workflows of one type in a single batch request.
    ///
    /// Much faster than calling [`Client::start_workflow`] once per input.
    /// The server generates each workflow ID. Inputs are sent as plain JSON,
    /// without the Payload envelope or codec chain, and the options'
    /// `workflow_id`, `cron_schedule` and `id_reuse_policy` are not applied.
    ///
    /// # Errors
    ///
    /// Fails if an input cannot be serialized or the server rejects the batch.
    pub async fn batch_start_workflows<I: Serialize>(
        &self,
        workflow_type: impl Into<String>,
        inputs: Vec<I>,
        options: StartWorkflowOptions,
    ) -> Result<Vec<WorkflowHandle>> {
        let workflow_type = workflow_type.into();
        let namespace = options
            .namespace
            .as_ref()
            .unwrap_or(&self.config.namespace)
            .clone();

        let mut requests = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let input_bytes = serde_json::to_vec(input).map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to serialize input: {}",
                    e
                )))
            })?;

            requests.push(orcher_sdk_core::proto::orcher::v1::StartWorkflowRequest {
                workflow_id: String::new(), // server generates
                workflow_type: workflow_type.clone(),
                task_queue: options.task_queue.clone(),
                namespace: namespace.clone(),
                input: input_bytes,
                execution_timeout: options.workflow_execution_timeout.map(proto_duration),
                run_timeout: options.workflow_run_timeout.map(proto_duration),
                task_timeout: options.workflow_task_timeout.map(proto_duration),
                retry_policy: options.retry_policy.as_ref().map(proto_retry_policy),
                cron_schedule: String::new(),
                annotations: std::collections::HashMap::new(),
                labels: std::collections::HashMap::new(),
                request_id: uuid::Uuid::new_v4().to_string(),
                start_delay: None,
                schedule_config: None,
                workflow_id_reuse_policy: 0,
                ..Default::default()
            });
        }

        let client = self.core_client.as_ref().clone().with_namespace(&namespace);
        let core_handles = client.batch_start_workflows(requests).await.map_err(|e| {
            Error::Client(crate::error::ClientError::RequestFailed(format!(
                "Failed to batch start workflows: {}",
                e
            )))
        })?;

        let handles = core_handles
            .into_iter()
            .map(|h| {
                WorkflowHandle::new_with_namespace(
                    Arc::clone(&self.core_client),
                    h.workflow_id().to_string(),
                    Some(h.run_id().to_string()),
                    Some(namespace.clone()),
                )
            })
            .collect();

        Ok(handles)
    }

    // ============================================================================
    // Getting Workflow Handles
    // ============================================================================

    /// Returns a handle to the latest run of an existing workflow.
    ///
    /// No request is made; an unknown ID surfaces on the first call through
    /// the handle.
    pub async fn get_workflow_handle(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<WorkflowHandle> {
        self.get_workflow_handle_with_run_id(workflow_id, None)
            .await
    }

    /// Returns a handle to a specific run, or to the latest run if `run_id` is `None`.
    ///
    /// No request is made; the workflow is checked on the first call through
    /// the handle. The handle uses the client's default namespace.
    pub async fn get_workflow_handle_with_run_id(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<WorkflowHandle> {
        let workflow_id = workflow_id.into();

        tracing::debug!(
            workflow_id = %workflow_id,
            run_id = ?run_id,
            "Getting workflow handle"
        );

        Ok(WorkflowHandle::new_with_namespace(
            Arc::clone(&self.core_client),
            workflow_id,
            run_id,
            None,
        ))
    }

    // ============================================================================
    // Direct Operations
    // ============================================================================

    /// Queries a workflow without first getting a handle.
    pub async fn query_workflow<Q, R>(
        &self,
        workflow_id: impl Into<String>,
        query: impl Into<String>,
        args: Q,
    ) -> Result<R>
    where
        Q: Serialize,
        R: serde::de::DeserializeOwned,
    {
        let handle = self.get_workflow_handle(workflow_id).await?;
        handle.query(query, args).await
    }

    // ============================================================================
    // Actor Operations
    // ============================================================================

    /// Returns a typed client for the actor instance identified by `key`.
    pub fn actor<A: ActorRef>(&self, key: impl Into<String>) -> A::Client {
        let handle = ActorInvocationClient::new(
            self.actor_client.clone(),
            A::actor_name().to_string(),
            key.into(),
        );
        A::client(handle)
    }

    /// Invokes an actor operation by name and waits for its result.
    ///
    /// This is the untyped form; prefer [`Client::actor`] when a typed actor
    /// is available.
    pub async fn invoke_actor<I, O>(
        &self,
        actor_name: impl Into<String>,
        key: impl Into<String>,
        operation: impl Into<String>,
        input: I,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let handle =
            ActorInvocationClient::new(self.actor_client.clone(), actor_name.into(), key.into());
        handle.invoke(&operation.into(), &(input,), None).await
    }

    /// Sends an actor operation without waiting for it to run.
    ///
    /// The server still executes the operation; only the caller does not wait.
    pub async fn send_actor<I>(
        &self,
        actor_name: impl Into<String>,
        key: impl Into<String>,
        operation: impl Into<String>,
        input: I,
    ) -> Result<()>
    where
        I: Serialize,
    {
        let handle =
            ActorInvocationClient::new(self.actor_client.clone(), actor_name.into(), key.into());
        handle.send(&operation.into(), &(input,), None).await
    }

    // ============================================================================
    // Listing & Searching Workflows
    // ============================================================================

    /// Lists workflow executions, one page at a time, with optional filters.
    pub async fn list_workflows(
        &self,
        options: orcher_sdk_core::ListWorkflowsOptions,
    ) -> Result<orcher_sdk_core::WorkflowListPage> {
        let client = self.core_client.as_ref().clone();
        client.list_workflows(options).await.map_err(|e| {
            Error::Client(crate::error::ClientError::RequestFailed(format!(
                "Failed to list workflows: {}",
                e
            )))
        })
    }

    /// Searches workflow executions with an SQL-like query.
    pub async fn search_workflows(
        &self,
        query: impl Into<String>,
        options: orcher_sdk_core::SearchWorkflowsOptions,
    ) -> Result<orcher_sdk_core::WorkflowListPage> {
        let client = self.core_client.as_ref().clone();
        client.search_workflows(query, options).await.map_err(|e| {
            Error::Client(crate::error::ClientError::RequestFailed(format!(
                "Failed to search workflows: {}",
                e
            )))
        })
    }

    // ============================================================================
    // Namespace Management
    // ============================================================================

    /// Creates a namespace.
    ///
    /// A namespace must exist before workflows can be started in it.
    pub async fn create_namespace(
        &self,
        name: impl Into<String>,
        retention_days: i32,
    ) -> Result<orcher_sdk_core::proto::orcher::v1::NamespaceInfo> {
        self.namespace_client
            .create_namespace(name, retention_days)
            .await
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to create namespace: {}",
                    e
                )))
            })
    }

    /// Fetches a namespace by name.
    pub async fn get_namespace(
        &self,
        name: impl Into<String>,
    ) -> Result<orcher_sdk_core::proto::orcher::v1::NamespaceInfo> {
        self.namespace_client
            .get_namespace(name)
            .await
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to get namespace: {}",
                    e
                )))
            })
    }

    /// Lists namespaces, returning at most the first 100.
    pub async fn list_namespaces(
        &self,
    ) -> Result<Vec<orcher_sdk_core::proto::orcher::v1::NamespaceInfo>> {
        let (namespaces, _total) = self
            .namespace_client
            .list_namespaces(100, 0)
            .await
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to list namespaces: {}",
                    e
                )))
            })?;
        Ok(namespaces)
    }

    /// Updates a namespace's description.
    ///
    /// The request sends the owner as empty and the retention period as zero.
    pub async fn update_namespace(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<orcher_sdk_core::proto::orcher::v1::NamespaceInfo> {
        self.namespace_client
            .update_namespace(name, description, "", 0)
            .await
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to update namespace: {}",
                    e
                )))
            })
    }

    /// Deprecates a namespace, so no new workflows can be started in it.
    pub async fn deprecate_namespace(&self, name: impl Into<String>) -> Result<()> {
        self.namespace_client
            .deprecate_namespace(name)
            .await
            .map(|_| ())
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to deprecate namespace: {}",
                    e
                )))
            })
    }

    /// Soft-deletes a namespace.
    pub async fn delete_namespace(&self, name: impl Into<String>) -> Result<()> {
        self.namespace_client
            .delete_namespace(name)
            .await
            .map_err(|e| {
                Error::Client(crate::error::ClientError::RequestFailed(format!(
                    "Failed to delete namespace: {}",
                    e
                )))
            })
    }

    /// The configuration this client was built with.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// The underlying sdk-core gRPC client.
    pub fn core_client(&self) -> &CoreClient {
        &self.core_client
    }

    /// The converter that encodes workflow inputs.
    pub fn data_converter(&self) -> &DataConverterType {
        &self.data_converter
    }

    /// Name of the encoding this client's converter produces.
    pub fn encoding(&self) -> &str {
        self.data_converter.encoding()
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("server_url", &self.config.server_url)
            .field("namespace", &self.config.namespace)
            .field("timeout", &self.config.timeout)
            .field("use_payload_format", &self.config.use_payload_format)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_client_debug() {
        let config = ClientConfig::default();
        // A real client needs a server, so this checks the config it would use.
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
    }

    #[test]
    fn test_client_config_access() {
        let config = ClientConfig {
            server_url: "http://test:1234".to_string(),
            namespace: "test-ns".to_string(),
            timeout: Duration::from_secs(10),
            identity: "test-client".to_string(),
            use_payload_format: true,
            data_converter: None,
            codec_chain: None,
            api_key: None,
            organization_id: None,
            tls: None,
        };

        assert_eq!(config.server_url, "http://test:1234");
        assert_eq!(config.namespace, "test-ns");
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.identity, "test-client");
        assert!(config.use_payload_format);
    }

    #[tokio::test]
    async fn with_api_key_after_connecting_changes_the_credentials_sent() {
        let client = Client::connect("http://127.0.0.1:1").await.unwrap();
        assert_eq!(client.core_client.auth().api_key, None);

        let client = client.use_binary_serialization().with_api_key("key-123");

        assert_eq!(
            client.core_client.auth().api_key.as_deref(),
            Some("key-123")
        );
        // The binary converter chosen before survives the rebuild.
        assert_eq!(client.data_converter.encoding(), "binary");
    }

    #[tokio::test]
    async fn an_unusable_client_certificate_is_rejected_at_connect() {
        let mut tls = crate::client::ClientTlsConfig::new();
        tls.client_key = Some(b"key".to_vec());
        let config = ClientConfig::new("https://orcher.example").with_tls(tls);
        let Err(err) = Client::with_config(config).await else {
            panic!("must fail");
        };
        assert!(err.to_string().contains("must be set together"), "{err}");
    }

    #[test]
    fn test_data_converter_type_default() {
        let converter = DataConverterType::default();
        assert_eq!(converter.encoding(), "json");
    }

    #[test]
    fn test_data_converter_type_json() {
        let converter = DataConverterType::Json(JsonPayloadConverter);
        let payload = converter.to_payload(&42).unwrap();
        assert!(payload.is_json());
        assert_eq!(converter.encoding(), "json");
    }

    #[test]
    fn test_data_converter_type_binary() {
        let converter = DataConverterType::Binary(BinaryPayloadConverter);
        assert_eq!(converter.encoding(), "binary");
    }

    #[test]
    fn test_client_config_with_custom_converter() {
        let config = ClientConfig {
            data_converter: Some(DataConverterType::Binary(BinaryPayloadConverter)),
            ..Default::default()
        };
        assert!(config.data_converter.is_some());
    }

    #[test]
    fn test_client_config_payload_format_enabled_by_default() {
        let config = ClientConfig::default();
        assert!(config.use_payload_format);
    }
}
