//! Options and configuration types for the client API.

use std::sync::Arc;
use std::time::Duration;

/// Configuration for the client.
#[derive(Clone)]
#[non_exhaustive]
pub struct ClientConfig {
    /// Server URL, for example `http://localhost:50051`.
    pub server_url: String,

    /// Namespace used when a call does not name one.
    pub namespace: String,

    /// Timeout applied to each request.
    pub timeout: Duration,

    /// Identity reported to the server, for observability.
    pub identity: String,

    /// Wrap inputs and outputs in Payload envelopes (default: true).
    ///
    /// Envelopes carry encoding metadata alongside the data. When disabled,
    /// values are sent as plain JSON.
    pub use_payload_format: bool,

    /// Converter that serializes workflow inputs, outputs and events.
    ///
    /// `None` uses the JSON payload converter. Supply your own to change the
    /// serialization format.
    pub data_converter: Option<crate::client::client::DataConverterType>,

    /// Codec chain applied to workflow inputs and outputs (default: None).
    ///
    /// Use it to compress payloads (for example with gzip) or to encrypt them
    /// in transit (for example with AES-256-GCM).
    ///
    /// The server decodes payloads and stores plain JSON so that they stay
    /// queryable; this is not end-to-end encryption.
    pub codec_chain: Option<Arc<orcher_sdk_core::codec::CodecChain>>,

    /// API key that authenticates with the server (default: None).
    ///
    /// When set, every gRPC request carries an `authorization: Bearer <key>` header.
    /// Obtain a key by registering a service with the orchestrator.
    pub api_key: Option<String>,

    /// Organization to act for (default: None).
    ///
    /// Sent as `x-organization-id` on every call. Needed only when the API key
    /// does not itself belong to an organization. A key that does is always
    /// scoped to its own organization, and the server refuses a different one.
    pub organization_id: Option<String>,

    /// TLS configuration (default: None).
    ///
    /// `None` follows the URL scheme: `https://` connects over TLS verified
    /// against the system trust store, `http://` connects in plaintext. Set it
    /// to trust a private CA, present a client certificate (mTLS) or override
    /// the server name. TLS is only negotiated for `https://` addresses.
    pub tls: Option<ClientTlsConfig>,
}

/// TLS settings for connecting to the ORCHER server.
///
/// Used by both [`ClientConfig::with_tls`] and
/// [`WorkerBuilder::tls`](crate::worker::WorkerBuilder::tls). An `https://`
/// address needs none of this to connect over TLS; supply it to trust a private
/// CA, present a client certificate (mTLS) or override the server name. With
/// `ca_cert: None` the server is verified against the system trust store, so a
/// publicly-trusted server certificate needs no CA material.
///
/// PEM can be given as bytes (`with_ca_cert`, `with_client_identity`) or read
/// from files (`with_ca_cert_file`, `with_client_identity_files`).
///
/// # Example — TLS against a publicly-trusted server
///
/// ```rust
/// # use orcher_sdk::client::ClientTlsConfig;
/// let tls = ClientTlsConfig::new(); // system trust store
/// ```
///
/// # Example — mTLS against a publicly-trusted server
///
/// Only the client identity is custom; the server is still verified against the
/// system trust store.
///
/// ```rust,no_run
/// # use orcher_sdk::client::ClientTlsConfig;
/// let tls = ClientTlsConfig::new().with_client_identity(
///     std::fs::read("client.pem").unwrap(),
///     std::fs::read("client-key.pem").unwrap(),
/// );
/// ```
///
/// # Example — mTLS against a private CA
///
/// ```rust,no_run
/// # use orcher_sdk::client::ClientTlsConfig;
/// # fn main() -> std::io::Result<()> {
/// let tls = ClientTlsConfig::new()
///     .with_ca_cert_file("ca.pem")?
///     .with_client_identity_files("client.pem", "client-key.pem")?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ClientTlsConfig {
    /// CA certificate (PEM) that verifies the server.
    ///
    /// `None` uses the system trust store, which is right for a publicly-trusted
    /// server certificate, including mTLS setups where only the client identity
    /// is custom. Supply a CA only for a private or self-signed authority.
    pub ca_cert: Option<Vec<u8>>,

    /// Client certificate (PEM), for mTLS.
    pub client_cert: Option<Vec<u8>>,

    /// Client private key (PEM), for mTLS.
    pub client_key: Option<Vec<u8>>,

    /// Domain name to use for SNI and certificate verification.
    pub domain_name: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            timeout: Duration::from_secs(30),
            identity: format!("orcher-client-{}", uuid::Uuid::new_v4()),
            use_payload_format: true,
            data_converter: None,  // JSON payload converter
            codec_chain: None,     // no compression or encryption
            api_key: None,         // unauthenticated
            organization_id: None, // scoped by the key, if at all
            tls: None,             // plain TCP
        }
    }
}

impl ClientConfig {
    /// Default configuration pointed at `server_url`.
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            ..Self::default()
        }
    }

    /// Set the server URL.
    pub fn with_server_url(mut self, server_url: impl Into<String>) -> Self {
        self.server_url = server_url.into();
        self
    }

    /// Set the default namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the identity reported for observability.
    pub fn with_identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = identity.into();
        self
    }

    /// Enable or disable the Payload envelope format.
    pub fn with_payload_format(mut self, use_payload_format: bool) -> Self {
        self.use_payload_format = use_payload_format;
        self
    }

    /// Set the data converter.
    pub fn with_data_converter(
        mut self,
        data_converter: crate::client::client::DataConverterType,
    ) -> Self {
        self.data_converter = Some(data_converter);
        self
    }

    /// Set the codec chain for compression/encryption.
    pub fn with_codec_chain(
        mut self,
        codec_chain: Arc<orcher_sdk_core::codec::CodecChain>,
    ) -> Self {
        self.codec_chain = Some(codec_chain);
        self
    }

    /// Authenticate with this API key.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Act for this organization.
    pub fn with_organization_id(mut self, organization_id: impl Into<String>) -> Self {
        self.organization_id = Some(organization_id.into());
        self
    }

    /// Connect over TLS.
    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

impl ClientTlsConfig {
    /// TLS that verifies the server against the system trust store and
    /// presents no client identity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verify the server against this CA (PEM) instead of the system trust store.
    pub fn with_ca_cert(mut self, ca_cert: impl Into<Vec<u8>>) -> Self {
        self.ca_cert = Some(ca_cert.into());
        self
    }

    /// Present this client certificate and key (PEM) for mTLS.
    pub fn with_client_identity(
        mut self,
        client_cert: impl Into<Vec<u8>>,
        client_key: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert = Some(client_cert.into());
        self.client_key = Some(client_key.into());
        self
    }

    /// Override the domain name used for SNI and certificate verification.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    /// Verify the server against the CA in this PEM file instead of the
    /// system trust store.
    ///
    /// # Errors
    ///
    /// Returns the I/O error, naming the path, if the file cannot be read.
    pub fn with_ca_cert_file(self, path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(self.with_ca_cert(read_pem(path.as_ref())?))
    }

    /// Present the client certificate and key in these PEM files, for mTLS.
    ///
    /// # Errors
    ///
    /// Returns the I/O error, naming the path, if either file cannot be read.
    pub fn with_client_identity_files(
        self,
        cert_path: impl AsRef<std::path::Path>,
        key_path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self> {
        let cert = read_pem(cert_path.as_ref())?;
        let key = read_pem(key_path.as_ref())?;
        Ok(self.with_client_identity(cert, key))
    }
}

/// Reads a PEM file, naming it in the error.
fn read_pem(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        std::io::Error::new(e.kind(), format!("cannot read {}: {}", path.display(), e))
    })
}

impl std::fmt::Debug for ClientTlsConfig {
    /// Shows which settings are present without printing key material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTlsConfig")
            .field("ca_cert", &self.ca_cert.as_ref().map(|_| "<pem>"))
            .field("client_cert", &self.client_cert.as_ref().map(|_| "<pem>"))
            .field(
                "client_key",
                &self.client_key.as_ref().map(|_| "<redacted>"),
            )
            .field("domain_name", &self.domain_name)
            .finish()
    }
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("server_url", &self.server_url)
            .field("namespace", &self.namespace)
            .field("timeout", &self.timeout)
            .field("identity", &self.identity)
            .field("use_payload_format", &self.use_payload_format)
            .field("data_converter", &"<DataConverter>")
            .field(
                "codec_chain",
                &if self.codec_chain.is_some() {
                    "<CodecChain>"
                } else {
                    "None"
                },
            )
            .field(
                "api_key",
                &if self.api_key.is_some() {
                    "<redacted>"
                } else {
                    "None"
                },
            )
            .field("organization_id", &self.organization_id)
            .field(
                "tls",
                &if self.tls.is_some() {
                    "<configured>"
                } else {
                    "None"
                },
            )
            .finish()
    }
}

/// Options for starting a workflow.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StartWorkflowOptions {
    /// Workflow ID; generated when unset.
    pub workflow_id: Option<String>,

    /// Task queue to run the workflow on.
    pub task_queue: String,

    /// Limit on the whole workflow execution, across runs.
    pub workflow_execution_timeout: Option<Duration>,

    /// Limit on a single run of the workflow.
    pub workflow_run_timeout: Option<Duration>,

    /// Limit on an individual workflow task.
    pub workflow_task_timeout: Option<Duration>,

    /// Namespace; the client's default when unset.
    pub namespace: Option<String>,

    /// Idempotency key for detecting duplicate starts.
    pub idempotency_key: Option<String>,

    /// Cron schedule, for recurring workflows.
    pub cron_schedule: Option<String>,

    /// Workflow-level retry policy (opt-in).
    ///
    /// When `None`, a failed workflow is **not** retried; recover with task-level
    /// retry plus compensation. With `max_attempts > 0`, a failed workflow is retried
    /// up to `max_attempts` times, each as a fresh run with its own event history.
    pub retry_policy: Option<crate::task::RetryPolicy>,

    /// What to do when `workflow_id` is already in use in the namespace.
    ///
    /// A workflow id names at most one open run. Starting an id whose run is
    /// still open fails with [`ClientError::WorkflowAlreadyExists`], naming
    /// that run, under every policy except
    /// [`WorkflowIdReusePolicy::TerminateIfRunning`]. `None` leaves the rest
    /// to the server, which allows a new run once the previous one has closed.
    ///
    /// [`ClientError::WorkflowAlreadyExists`]: crate::error::ClientError::WorkflowAlreadyExists
    pub id_reuse_policy: Option<WorkflowIdReusePolicy>,
}

impl StartWorkflowOptions {
    /// Options to start a workflow on `task_queue`, everything else unset.
    pub fn new(task_queue: impl Into<String>) -> Self {
        Self {
            task_queue: task_queue.into(),
            ..Self::default()
        }
    }

    /// Set the workflow ID (generated when unset).
    pub fn with_workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = Some(workflow_id.into());
        self
    }

    /// Set the task queue.
    pub fn with_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = task_queue.into();
        self
    }

    /// Bound the whole workflow execution.
    pub fn with_workflow_execution_timeout(mut self, timeout: Duration) -> Self {
        self.workflow_execution_timeout = Some(timeout);
        self
    }

    /// Bound a single run of the workflow.
    pub fn with_workflow_run_timeout(mut self, timeout: Duration) -> Self {
        self.workflow_run_timeout = Some(timeout);
        self
    }

    /// Bound an individual workflow task.
    pub fn with_workflow_task_timeout(mut self, timeout: Duration) -> Self {
        self.workflow_task_timeout = Some(timeout);
        self
    }

    /// Start in this namespace instead of the client's default.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set an idempotency key for duplicate detection.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Run the workflow on a cron schedule.
    pub fn with_cron_schedule(mut self, cron_schedule: impl Into<String>) -> Self {
        self.cron_schedule = Some(cron_schedule.into());
        self
    }

    /// Retry a failed workflow as a fresh run under this policy.
    pub fn with_retry_policy(mut self, retry_policy: crate::task::RetryPolicy) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Say what to do when the workflow ID is already in use.
    pub fn with_id_reuse_policy(mut self, policy: WorkflowIdReusePolicy) -> Self {
        self.id_reuse_policy = Some(policy);
        self
    }
}

/// Whether a start may reuse a workflow id that an earlier run already carried.
///
/// An open run refuses a start under every policy except `TerminateIfRunning`;
/// the policies differ in what they allow once that run has closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowIdReusePolicy {
    /// Start once the previous run has closed, however it closed.
    AllowDuplicate,
    /// Start once the previous run has closed without completing: failed,
    /// cancelled, terminated or timed out.
    AllowDuplicateFailedOnly,
    /// Never start an id that any run has carried before.
    RejectDuplicate,
    /// Terminate the open run, if there is one, and start.
    TerminateIfRunning,
}

impl WorkflowIdReusePolicy {
    /// Converts to the protobuf enum sent on the wire.
    pub(crate) fn to_proto(self) -> orcher_sdk_core::proto::orcher::v1::WorkflowIdReusePolicy {
        use orcher_sdk_core::proto::orcher::v1::WorkflowIdReusePolicy as Proto;
        match self {
            Self::AllowDuplicate => Proto::AllowDuplicate,
            Self::AllowDuplicateFailedOnly => Proto::AllowDuplicateFailedOnly,
            Self::RejectDuplicate => Proto::RejectDuplicate,
            Self::TerminateIfRunning => Proto::TerminateIfRunning,
        }
    }
}

/// Options for querying a workflow.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryOptions {
    /// Timeout for the query.
    pub timeout: Option<Duration>,

    /// Condition under which the server rejects the query instead of running it.
    pub reject_condition: Option<QueryRejectCondition>,
}

impl QueryOptions {
    /// Set the query timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Reject the query unless the workflow meets this condition.
    pub fn with_reject_condition(mut self, condition: QueryRejectCondition) -> Self {
        self.reject_condition = Some(condition);
        self
    }
}

/// Workflow states in which a query is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueryRejectCondition {
    /// Reject if the workflow is closed (for example completed or failed).
    NotOpen,
    /// Reject unless the workflow has completed.
    NotCompleted,
}

/// Options for sending an event to a workflow.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct EventOptions {
    /// Timeout for delivering the event.
    pub timeout: Option<Duration>,
}

impl EventOptions {
    /// Set the event timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_defaults() {
        let config = ClientConfig::default();
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert!(config.identity.starts_with("orcher-client-"));
        assert!(config.use_payload_format);
        assert!(config.data_converter.is_none());
    }

    #[test]
    fn test_client_config_with_payload_format_disabled() {
        let config = ClientConfig {
            use_payload_format: false,
            ..Default::default()
        };
        assert!(!config.use_payload_format);
    }

    #[test]
    fn test_client_config_debug() {
        let config = ClientConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("use_payload_format"));
        assert!(debug_str.contains("data_converter"));
    }

    #[test]
    fn test_start_workflow_options_defaults() {
        let options = StartWorkflowOptions::default();
        assert!(options.workflow_id.is_none());
        assert_eq!(options.task_queue, "");
        assert!(options.workflow_execution_timeout.is_none());
        assert!(options.namespace.is_none());
    }

    #[test]
    fn test_query_options_defaults() {
        let options = QueryOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.reject_condition.is_none());
    }

    #[test]
    fn test_query_reject_condition() {
        assert_eq!(QueryRejectCondition::NotOpen, QueryRejectCondition::NotOpen);
        assert_ne!(
            QueryRejectCondition::NotOpen,
            QueryRejectCondition::NotCompleted
        );
    }

    #[test]
    fn test_event_options_defaults() {
        let options = EventOptions::default();
        assert!(options.timeout.is_none());
    }
}
