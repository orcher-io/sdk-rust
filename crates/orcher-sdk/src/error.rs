//! Error types for the Orcher Rust SDK.
//!
//! [`Error`] wraps one enum per area: workflows, tasks, the client and the worker.
//!
//! ## Error Conversion
//!
//! Errors from `sdk-core` convert into [`Error`] through `From`:
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[derive(Deserialize)]
//! struct Config {
//!     retries: u32,
//! }
//!
//! fn parse(json: &str) -> Result<Config> {
//!     // `?` converts the `serde_json::Error` into an `orcher_sdk::Error`.
//!     Ok(serde_json::from_str(json)?)
//! }
//!
//! assert!(parse(r#"{"retries": 3}"#).is_ok());
//! assert!(matches!(parse("not json"), Err(Error::Serialization(_))));
//! ```
//!
//! ## Error Classification
//!
//! [`Error::is_retryable`] and [`Error::is_permanent`] classify an error. An error can be
//! neither, in which case the caller decides.
//!
//! ```rust
//! use orcher_sdk::error::{ClientError, Error};
//! use std::time::Duration;
//!
//! fn describe(error: &Error) -> &'static str {
//!     if error.is_retryable() {
//!         "retry the operation"
//!     } else if error.is_permanent() {
//!         "fatal, do not retry"
//!     } else {
//!         "the caller decides"
//!     }
//! }
//!
//! let timeout = Error::Client(ClientError::timeout(Duration::from_secs(5)));
//! assert_eq!(describe(&timeout), "retry the operation");
//!
//! let bad_config = Error::Configuration("missing server URL".to_string());
//! assert_eq!(describe(&bad_config), "fatal, do not retry");
//! ```

/// Result type alias for SDK operations.
pub type Result<T> = std::result::Result<T, Error>;

/// JSON marker key that encodes a terminal task failure as an injected replay result.
///
/// When a task fails terminally (retries exhausted, or moved to the dead-letter queue), the
/// server delivers it to the worker as a `CompleteStep{failure}` job. The worker injects a small
/// JSON object under this key, in the slot for that step, so that `execute_task`/`execute` decode
/// it on replay and raise a catchable [`WorkflowError::TaskFailed`] instead of deserializing it
/// as a success value. It mirrors the child-workflow failure marker. The key is part of the wire
/// format and must stay byte-identical across SDKs.
pub(crate) const TASK_FAILED_SENTINEL_KEY: &str = "__orcher_task_failed__";

/// Machine-readable error code, shared by the Rust, TypeScript and Python SDKs.
///
/// The `Display` form (for example `TASK_FAILED`) is the same string in every SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    // Workflow errors
    /// Workflow execution failed
    WorkflowFailed,
    /// Workflow not found
    WorkflowNotFound,
    /// Workflow already exists
    WorkflowAlreadyExists,
    /// Workflow was canceled
    WorkflowCanceled,
    /// Workflow timed out
    WorkflowTimeout,
    /// Non-deterministic workflow execution detected
    WorkflowNonDeterministic,
    /// Workflow state error
    WorkflowStateError,
    /// Workflow version mismatch
    WorkflowVersionMismatch,
    /// Workflow replay error
    WorkflowReplayError,

    // Task errors
    /// Task execution failed
    TaskFailed,
    /// Task not found
    TaskNotFound,
    /// Task timed out
    TaskTimeout,
    /// Task was canceled
    TaskCanceled,
    /// Task heartbeat failed
    TaskHeartbeatFailed,
    /// Task retry limit exceeded
    TaskRetryLimitExceeded,
    /// Invalid task input
    TaskInvalidInput,

    // Client errors
    /// Connection to server failed
    ConnectionFailed,
    /// Request timed out
    RequestTimeout,
    /// Authentication failed
    AuthenticationFailed,
    /// Authorization failed
    AuthorizationFailed,
    /// Server returned an error
    ServerError,
    /// Invalid request
    InvalidRequest,

    // Service errors
    /// Invalid configuration
    InvalidConfiguration,
    /// Service startup failed
    StartupFailed,
    /// Service shutdown error
    ShutdownError,
    /// Handler not found
    HandlerNotFound,
    /// Resource exhausted
    ResourceExhausted,
    /// Polling error
    PollingError,

    // General errors
    /// Serialization/deserialization error
    SerializationError,
    /// Network/transport error
    NetworkError,
    /// I/O error
    IoError,
    /// Internal/unknown error
    Internal,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::WorkflowFailed => "WORKFLOW_FAILED",
            Self::WorkflowNotFound => "WORKFLOW_NOT_FOUND",
            Self::WorkflowAlreadyExists => "WORKFLOW_ALREADY_EXISTS",
            Self::WorkflowCanceled => "WORKFLOW_CANCELED",
            Self::WorkflowTimeout => "WORKFLOW_TIMEOUT",
            Self::WorkflowNonDeterministic => "WORKFLOW_NON_DETERMINISTIC",
            Self::WorkflowStateError => "WORKFLOW_STATE_ERROR",
            Self::WorkflowVersionMismatch => "WORKFLOW_VERSION_MISMATCH",
            Self::WorkflowReplayError => "WORKFLOW_REPLAY_ERROR",
            Self::TaskFailed => "TASK_FAILED",
            Self::TaskNotFound => "TASK_NOT_FOUND",
            Self::TaskTimeout => "TASK_TIMEOUT",
            Self::TaskCanceled => "TASK_CANCELED",
            Self::TaskHeartbeatFailed => "TASK_HEARTBEAT_FAILED",
            Self::TaskRetryLimitExceeded => "TASK_RETRY_LIMIT_EXCEEDED",
            Self::TaskInvalidInput => "TASK_INVALID_INPUT",
            Self::ConnectionFailed => "CONNECTION_FAILED",
            Self::RequestTimeout => "REQUEST_TIMEOUT",
            Self::AuthenticationFailed => "AUTHENTICATION_FAILED",
            Self::AuthorizationFailed => "AUTHORIZATION_FAILED",
            Self::ServerError => "SERVER_ERROR",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::InvalidConfiguration => "INVALID_CONFIGURATION",
            Self::StartupFailed => "STARTUP_FAILED",
            Self::ShutdownError => "SHUTDOWN_ERROR",
            Self::HandlerNotFound => "HANDLER_NOT_FOUND",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::PollingError => "POLLING_ERROR",
            Self::SerializationError => "SERIALIZATION_ERROR",
            Self::NetworkError => "NETWORK_ERROR",
            Self::IoError => "IO_ERROR",
            Self::Internal => "INTERNAL",
        };
        write!(f, "{}", s)
    }
}

/// How urgently an error needs an operator's attention, shared by all SDKs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// Low severity — informational, operation can continue
    Low,
    /// Medium severity — degraded functionality, should investigate
    Medium,
    /// High severity — significant impact, needs attention
    High,
    /// Critical severity — service at risk, immediate action needed
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Critical => "CRITICAL",
        };
        write!(f, "{}", s)
    }
}

/// Main error type for the Orcher Rust SDK.
///
/// This enum, the per-area enums it wraps, and their variants with named
/// fields are all `#[non_exhaustive]`: a new kind of error, or a new detail on
/// an existing one, is not a breaking change. Match with a wildcard arm and
/// `..` in struct patterns, and build errors with the constructors such as
/// [`TaskError::application`] or [`ClientError::workflow_not_found`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Workflow execution error
    #[error("Workflow error: {0}")]
    Workflow(#[from] WorkflowError),

    /// Task execution error
    #[error("Task error: {0}")]
    Task(#[from] TaskError),

    /// Client operation error
    #[error("Client error: {0}")]
    Client(#[from] ClientError),

    /// Worker error
    #[error("Worker error: {0}")]
    Worker(#[from] WorkerError),

    /// Actor operation error
    #[error("Actor error: {0}")]
    Actor(String),

    /// Network/RPC error
    #[error("Network error: {0}")]
    Network(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Errors that can occur during workflow execution.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowError {
    /// A task executed by the workflow failed
    #[error("Task '{task_type}' failed after {attempts} attempts: {reason}")]
    #[non_exhaustive]
    TaskFailed {
        /// The type/name of the task that failed
        task_type: String,
        /// Number of retry attempts made
        attempts: u32,
        /// The reason for the failure
        reason: String,
    },

    /// A timer operation failed
    #[error("Timer '{timer_id}' failed: {reason}")]
    #[non_exhaustive]
    TimerFailed {
        /// The ID of the timer that failed
        timer_id: String,
        /// The reason for the failure
        reason: String,
    },

    /// A child workflow failed
    #[error("Child workflow '{workflow_type}' (ID: {workflow_id}) failed: {reason}")]
    #[non_exhaustive]
    ChildWorkflowFailed {
        /// The type of the child workflow
        workflow_type: String,
        /// The ID of the child workflow instance
        workflow_id: String,
        /// The reason for the failure
        reason: String,
    },

    /// Workflow state management error
    #[error("State error: {0}")]
    StateError(
        /// Description of the state error
        String,
    ),

    /// Workflow was canceled
    #[error("Workflow was canceled")]
    Canceled,

    /// Workflow timed out
    #[error("Workflow execution timed out after {timeout:?}")]
    #[non_exhaustive]
    Timeout {
        /// The timeout duration that was exceeded
        timeout: std::time::Duration,
    },

    /// Workflow panicked
    #[error("Workflow panicked: {0}")]
    Panic(
        /// The panic message
        String,
    ),

    /// Workflow execution is not deterministic
    #[error("Non-deterministic workflow execution detected: {0}")]
    NonDeterministic(
        /// Description of the non-determinism violation
        String,
    ),

    /// Event handling error
    #[error("Event '{event}' error: {reason}")]
    #[non_exhaustive]
    EventError {
        /// The name of the event that failed
        event: String,
        /// The reason for the failure
        reason: String,
    },

    /// Query handling error
    #[error("Query '{query}' error: {reason}")]
    #[non_exhaustive]
    QueryError {
        /// The name of the query that failed
        query: String,
        /// The reason for the failure
        reason: String,
    },

    /// Invalid workflow state
    #[error("Invalid workflow state: {0}")]
    InvalidState(
        /// Description of the invalid state
        String,
    ),

    /// Workflow not found
    #[error("Workflow '{workflow_id}' not found")]
    #[non_exhaustive]
    NotFound {
        /// The ID of the workflow that was not found
        workflow_id: String,
    },

    /// Replay error
    #[error("Replay error: {0}")]
    ReplayError(
        /// Description of the replay error
        String,
    ),

    /// Restarting the workflow fresh failed.
    #[error("Restart fresh error: {0}")]
    RestartFreshError(
        /// Description of the restart error
        String,
    ),

    /// Version mismatch error
    #[error("Version mismatch: expected {expected}, got {actual}")]
    #[non_exhaustive]
    VersionMismatch {
        /// The expected version
        expected: String,
        /// The actual version found
        actual: String,
    },

    /// The workflow is waiting for operations it started to complete.
    ///
    /// This is control flow, not a failure. A workflow returns it when it calls
    /// `ctx.execute()` and the result is not available yet. The workflow executor
    /// catches it, sends the pending commands to the server, and replays the
    /// workflow once results arrive. See [`Error::is_suspend`].
    #[error("Workflow suspended: {reason}")]
    #[non_exhaustive]
    Suspended {
        /// Human-readable reason for suspension
        reason: String,
        /// List of pending operation IDs (task IDs, timer IDs, etc.)
        pending_operations: Vec<String>,
    },
}

/// Errors that can occur during task execution.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum TaskError {
    /// Task execution failed
    #[error("Task execution failed: {0}")]
    ExecutionFailed(
        /// Description of the execution failure
        String,
    ),

    /// Task timed out
    #[error("Task execution timed out after {timeout:?}")]
    #[non_exhaustive]
    Timeout {
        /// The timeout duration that was exceeded
        timeout: std::time::Duration,
    },

    /// Task was canceled
    #[error("Task was canceled")]
    Canceled,

    /// Heartbeat failed
    #[error("Heartbeat failed: {0}")]
    HeartbeatFailed(
        /// Details about the heartbeat failure
        String,
    ),

    /// Heartbeat timeout (no heartbeat received)
    #[error("No heartbeat received within {timeout:?}")]
    #[non_exhaustive]
    HeartbeatTimeout {
        /// The timeout duration that was exceeded
        timeout: std::time::Duration,
    },

    /// Task not found
    #[error("Task '{task_id}' not found")]
    #[non_exhaustive]
    NotFound {
        /// The ID of the task that was not found
        task_id: String,
    },

    /// Task already completed
    #[error("Task '{task_id}' already completed")]
    #[non_exhaustive]
    AlreadyCompleted {
        /// The ID of the task that was already completed
        task_id: String,
    },

    /// Task panic
    #[error("Task panicked: {0}")]
    Panic(
        /// The panic message
        String,
    ),

    /// Retry limit exceeded
    #[error("Task failed after {attempts} retry attempts")]
    #[non_exhaustive]
    RetryLimitExceeded {
        /// Number of retry attempts that were made
        attempts: u32,
    },

    /// Invalid task input
    #[error("Invalid task input: {0}")]
    InvalidInput(
        /// Description of the invalid input
        String,
    ),

    /// Invalid task configuration
    #[error("Invalid task configuration: {0}")]
    InvalidConfiguration(
        /// Description of the configuration error
        String,
    ),

    /// A failure the task's own code names, reported to the engine under
    /// `error_type` so a retry policy's non-retryable list can match it.
    /// Build one with [`TaskError::application`] or
    /// [`TaskError::non_retryable`].
    #[error("{message}")]
    #[non_exhaustive]
    Application {
        /// The name a retry policy lists the failure under.
        error_type: String,
        /// What went wrong.
        message: String,
        /// No retry can succeed: the engine does not retry the task,
        /// whatever its policy allows.
        non_retryable: bool,
    },
}

/// Errors that can occur during client operations.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Connection to server failed
    #[error("Failed to connect to server at '{url}': {reason}")]
    #[non_exhaustive]
    ConnectionFailed {
        /// The server URL that failed to connect
        url: String,
        /// The reason for connection failure
        reason: String,
    },

    /// Request failed
    #[error("Request failed: {0}")]
    RequestFailed(
        /// Details about the request failure
        String,
    ),

    /// Request timed out
    #[error("Request timed out after {timeout:?}")]
    #[non_exhaustive]
    Timeout {
        /// The timeout duration that was exceeded
        timeout: std::time::Duration,
    },

    /// Invalid server URL
    #[error("Invalid server URL: {0}")]
    InvalidUrl(
        /// Description of the invalid URL
        String,
    ),

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(
        /// Details about the authentication failure
        String,
    ),

    /// Authorization failed
    #[error("Authorization failed: {0}")]
    AuthorizationFailed(
        /// Details about the authorization failure
        String,
    ),

    /// Workflow not found
    #[error("Workflow '{workflow_id}' not found")]
    #[non_exhaustive]
    WorkflowNotFound {
        /// The ID of the workflow that was not found
        workflow_id: String,
    },

    /// The workflow id is already in use: its run is still open, or a closed
    /// one the start's reuse policy does not allow reusing.
    #[error("Workflow '{workflow_id}' already exists")]
    #[non_exhaustive]
    WorkflowAlreadyExists {
        /// The ID of the workflow that already exists
        workflow_id: String,
        /// The run in the way, when the server named it: the one to wait on
        /// for a caller that meant to start the workflow once.
        run_id: Option<String>,
    },

    /// Invalid workflow input
    #[error("Invalid workflow input: {0}")]
    InvalidInput(
        /// Description of the invalid input
        String,
    ),

    /// Invalid response from server
    #[error("Invalid response from server: {0}")]
    InvalidResponse(
        /// Description of the invalid response
        String,
    ),

    /// Server returned an error
    #[error("Server error (status {status}): {message}")]
    #[non_exhaustive]
    ServerError {
        /// HTTP status code
        status: u16,
        /// Error message from server
        message: String,
    },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(
        /// Details about the serialization error
        String,
    ),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(
        /// Details about the deserialization error
        String,
    ),
}

/// Errors that can occur in a worker.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum WorkerError {
    /// Worker configuration is invalid
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(
        /// Description of the configuration error
        String,
    ),

    /// Worker failed to start
    #[error("Failed to start service: {0}")]
    StartupFailed(
        /// Details about the startup failure
        String,
    ),

    /// Worker already running
    #[error("Worker is already running")]
    AlreadyRunning,

    /// Worker not running
    #[error("Worker is not running")]
    NotRunning,

    /// Failed to connect to server
    #[error("Failed to connect to server at '{url}': {reason}")]
    #[non_exhaustive]
    ConnectionFailed {
        /// The server URL that failed to connect
        url: String,
        /// The reason for connection failure
        reason: String,
    },

    /// Polling error
    #[error("Polling error: {0}")]
    PollingError(
        /// Details about the polling error
        String,
    ),

    /// Handler registration error
    #[error("Handler registration error: {0}")]
    RegistrationError(
        /// Details about the registration error
        String,
    ),

    /// Handler not found
    #[error("No handler registered for '{handler_type}' with name '{name}'")]
    #[non_exhaustive]
    HandlerNotFound {
        /// The type of handler (e.g., "workflow", "task")
        handler_type: String,
        /// The name of the handler that was not found
        name: String,
    },

    /// Handler execution error
    #[error("Handler execution error: {0}")]
    HandlerExecutionError(
        /// Details about the handler execution error
        String,
    ),

    /// Shutdown error
    #[error("Shutdown error: {0}")]
    ShutdownError(
        /// Details about the shutdown error
        String,
    ),

    /// Shutdown timeout
    #[error("Shutdown timed out after {timeout:?}")]
    #[non_exhaustive]
    ShutdownTimeout {
        /// The timeout duration that was exceeded
        timeout: std::time::Duration,
    },

    /// Resource exhausted
    #[error("Resource exhausted: {resource}")]
    #[non_exhaustive]
    ResourceExhausted {
        /// The name of the exhausted resource
        resource: String,
    },

    /// Task queue not found
    #[error("Task queue '{queue}' not found")]
    #[non_exhaustive]
    TaskQueueNotFound {
        /// The name of the task queue that was not found
        queue: String,
    },
}

// ============================================================================
// Error Conversions
// ============================================================================

/// Translate a gRPC status code to its conventional HTTP equivalent.
///
/// This exists so that error classification (retryable or not) and the status a
/// user sees are both meaningful. The mapping is the standard one. The cases that
/// matter most are UNAVAILABLE and DEADLINE_EXCEEDED landing in 5xx so they are
/// retried, and NOT_FOUND landing in 4xx so it is not.
fn grpc_code_to_http_status(code: tonic::Code) -> u16 {
    use tonic::Code;
    match code {
        Code::Ok => 200,
        Code::InvalidArgument | Code::OutOfRange | Code::FailedPrecondition => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::Cancelled => 499,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::DeadlineExceeded => 504,
        // Internal, Unknown, DataLoss and anything new default to 500, which is
        // retryable — the safer side for a transient server-side failure.
        _ => 500,
    }
}

impl From<orcher_sdk_core::error::Error> for Error {
    fn from(err: orcher_sdk_core::error::Error) -> Self {
        use orcher_sdk_core::error::Error as CoreError;

        match err {
            // Workflow errors
            CoreError::WorkflowNotFound { workflow_id, .. } => {
                Error::Workflow(WorkflowError::NotFound { workflow_id })
            }
            CoreError::WorkflowAlreadyExists {
                workflow_id,
                run_id,
                ..
            } => Error::Client(ClientError::WorkflowAlreadyExists {
                workflow_id,
                run_id,
            }),
            CoreError::WorkflowExecutionFailed { message, .. } => {
                Error::Workflow(WorkflowError::Panic(message))
            }
            CoreError::WorkflowCancelled { .. } => Error::Workflow(WorkflowError::Canceled),
            CoreError::WorkflowTerminated { reason, .. } => Error::Workflow(WorkflowError::Panic(
                reason.unwrap_or_else(|| "Workflow terminated".to_string()),
            )),

            // Task errors
            CoreError::TaskExecutionFailed { reason, .. } => {
                Error::Task(TaskError::ExecutionFailed(reason))
            }
            CoreError::TaskCancelled { .. } => Error::Task(TaskError::Canceled),

            // State machine errors
            CoreError::InvalidWorkflowState {
                expected, actual, ..
            } => Error::Workflow(WorkflowError::InvalidState(format!(
                "Expected state: {}, got: {}",
                expected, actual
            ))),
            CoreError::DeterminismViolation { reason, .. } => {
                Error::Workflow(WorkflowError::NonDeterministic(reason))
            }
            CoreError::InvalidExecutionJournal { reason, .. } => {
                Error::Workflow(WorkflowError::ReplayError(reason))
            }
            CoreError::ReplayError(reason) => Error::Workflow(WorkflowError::ReplayError(reason)),

            // Serialization errors
            CoreError::Serialization(msg) => Error::Serialization(msg),
            CoreError::Deserialization(msg) => Error::Serialization(msg),
            CoreError::InvalidPayload { reason, .. } => Error::Serialization(reason),
            CoreError::Codec(msg) => Error::Serialization(msg),

            // gRPC and transport errors
            CoreError::Transport(err) => Error::Client(ClientError::ConnectionFailed {
                url: "unknown".to_string(),
                reason: err.to_string(),
            }),
            CoreError::GrpcStatus(status) => Error::Client(ClientError::ServerError {
                // gRPC codes are 0-16, but `is_retryable` classifies ServerError by
                // HTTP range. Storing the raw code would make every gRPC failure
                // non-retryable, including UNAVAILABLE and DEADLINE_EXCEEDED, so
                // translate to HTTP here at the boundary.
                status: grpc_code_to_http_status(status.code()),
                message: status.message().to_string(),
            }),

            // Connection and auth errors
            CoreError::Connection(msg) => Error::Client(ClientError::ConnectionFailed {
                url: "unknown".to_string(),
                reason: msg,
            }),
            CoreError::Authentication(msg) => Error::Client(ClientError::AuthenticationFailed(msg)),
            CoreError::Authorization(msg) => Error::Client(ClientError::AuthorizationFailed(msg)),

            // Configuration errors
            CoreError::Configuration(msg) => Error::Configuration(msg),

            // Worker errors
            CoreError::WorkerError(msg) => Error::Worker(WorkerError::PollingError(msg)),
            CoreError::PollerError(msg) => Error::Worker(WorkerError::PollingError(msg)),

            // Timeout errors
            CoreError::Timeout { .. } => {
                Error::Client(ClientError::Timeout {
                    timeout: std::time::Duration::from_secs(30), // Default
                })
            }

            // Resource exhaustion
            CoreError::ResourceExhausted { resource, .. } => {
                Error::Worker(WorkerError::ResourceExhausted { resource })
            }

            // Invalid commands/events
            CoreError::InvalidCommand { reason, .. } => {
                Error::Workflow(WorkflowError::InvalidState(reason))
            }
            CoreError::InvalidEvent { reason, .. } => Error::Workflow(WorkflowError::EventError {
                event: "unknown".to_string(),
                reason,
            }),

            // Internal errors
            CoreError::Internal(msg) => Error::Other(msg),
            CoreError::CacheError(msg) => Error::Other(msg),
            CoreError::StateMachineError(msg) => Error::Workflow(WorkflowError::InvalidState(msg)),

            // Context wrapper
            CoreError::WithContext {
                context, source, ..
            } => {
                let sdk_error: Error = (*source).into();
                Error::Other(format!("{}: {}", context, sdk_error))
            }

            // Passthrough
            CoreError::Other(err) => Error::Other(err.to_string()),

            // Any variant sdk-core adds later.
            other => Error::Other(other.to_string()),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Other(err.to_string())
    }
}

// ============================================================================
// Error Classification
// ============================================================================

impl Error {
    /// Whether this is the workflow suspension signal rather than a real error.
    ///
    /// Suspension is control flow: `ctx.execute_task`, `ctx.execute` and timers
    /// return [`WorkflowError::Suspended`] to hand control back to the runtime
    /// so it can schedule work and replay the workflow later. Combinators that
    /// inspect errors (sagas, `select`, retry wrappers) must propagate it
    /// unchanged and never treat it as a failed step.
    pub fn is_suspend(&self) -> bool {
        matches!(self, Error::Workflow(WorkflowError::Suspended { .. }))
    }

    /// Whether this error is transient and may succeed on retry.
    ///
    /// Examples are connection failures, timeouts, 5xx and 429 server
    /// responses, and task failures not marked non-retryable.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::error::{ClientError, Error};
    ///
    /// let error = Error::Client(ClientError::server_error(503, "unavailable"));
    /// assert!(error.is_retryable());
    ///
    /// let error = Error::Client(ClientError::server_error(400, "bad request"));
    /// assert!(!error.is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            // Client errors - connection and timeout are retryable
            Error::Client(ClientError::ConnectionFailed { .. }) => true,
            Error::Client(ClientError::Timeout { .. }) => true,
            Error::Client(ClientError::RequestFailed(_)) => true,
            Error::Client(ClientError::ServerError { status, .. }) => {
                // 5xx are retryable and 4xx are not, except 429 (gRPC
                // RESOURCE_EXHAUSTED), which asks the caller to back off rather
                // than rejecting the request for good.
                *status == 429 || (*status >= 500 && *status < 600)
            }

            // Service errors
            Error::Worker(WorkerError::PollingError(_)) => true,
            Error::Worker(WorkerError::ConnectionFailed { .. }) => true,
            Error::Worker(WorkerError::ResourceExhausted { .. }) => true,

            // Task errors - execution failures and timeouts are retryable
            Error::Task(TaskError::ExecutionFailed(_)) => true,
            Error::Task(TaskError::Timeout { .. }) => true,
            Error::Task(TaskError::HeartbeatTimeout { .. }) => true,
            Error::Task(TaskError::Application { non_retryable, .. }) => !non_retryable,

            // I/O errors are generally retryable
            Error::Io(_) => true,

            // Most other errors are not retryable
            _ => false,
        }
    }

    /// Whether this error is permanent, so retrying cannot fix it.
    ///
    /// Examples are invalid configuration, authentication failures,
    /// non-determinism and 4xx server responses other than 429.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::error::{Error, TaskError};
    ///
    /// let error: Error = TaskError::non_retryable("CardDeclined", "card declined").into();
    /// assert!(error.is_permanent());
    ///
    /// let error: Error = TaskError::application("Throttled", "slow down").into();
    /// assert!(!error.is_permanent());
    /// ```
    pub fn is_permanent(&self) -> bool {
        match self {
            // Configuration errors are permanent
            Error::Configuration(_) => true,

            // Workflow state errors are permanent
            Error::Workflow(WorkflowError::NonDeterministic(_)) => true,
            Error::Workflow(WorkflowError::VersionMismatch { .. }) => true,
            Error::Workflow(WorkflowError::InvalidState(_)) => true,

            // Task configuration errors are permanent
            Error::Task(TaskError::InvalidInput(_)) => true,
            Error::Task(TaskError::InvalidConfiguration(_)) => true,
            Error::Task(TaskError::RetryLimitExceeded { .. }) => true,
            Error::Task(TaskError::Application { non_retryable, .. }) => *non_retryable,

            // Client authentication/authorization errors are permanent
            Error::Client(ClientError::AuthenticationFailed(_)) => true,
            Error::Client(ClientError::AuthorizationFailed(_)) => true,
            Error::Client(ClientError::InvalidUrl(_)) => true,
            Error::Client(ClientError::InvalidInput(_)) => true,
            Error::Client(ClientError::WorkflowAlreadyExists { .. }) => true,

            // Service configuration errors are permanent
            Error::Worker(WorkerError::InvalidConfiguration(_)) => true,
            Error::Worker(WorkerError::HandlerNotFound { .. }) => true,

            // Client 4xx errors are permanent (except 429)
            Error::Client(ClientError::ServerError { status, .. }) => {
                *status >= 400 && *status < 500 && *status != 429
            }

            _ => false,
        }
    }

    /// Wrap this error with a context message.
    ///
    /// The result is an [`Error::Other`] whose message is `"{context}: {self}"`,
    /// so the original variant is not preserved for matching.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::error::{Error, TaskError};
    ///
    /// let error: Error = TaskError::execution_failed("card declined").into();
    /// let error = error.with_context("Failed to execute payment task");
    /// assert!(error.to_string().starts_with("Failed to execute payment task: "));
    /// ```
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Error::Other(format!("{}: {}", context.into(), self))
    }

    /// The [`ErrorCode`] for this error, the same in every SDK.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Workflow(w) => match w {
                WorkflowError::TaskFailed { .. } => ErrorCode::TaskFailed,
                WorkflowError::TimerFailed { .. } => ErrorCode::WorkflowFailed,
                WorkflowError::ChildWorkflowFailed { .. } => ErrorCode::WorkflowFailed,
                WorkflowError::StateError(_) => ErrorCode::WorkflowStateError,
                WorkflowError::Canceled => ErrorCode::WorkflowCanceled,
                WorkflowError::Timeout { .. } => ErrorCode::WorkflowTimeout,
                WorkflowError::Panic(_) => ErrorCode::WorkflowFailed,
                WorkflowError::NonDeterministic(_) => ErrorCode::WorkflowNonDeterministic,
                WorkflowError::EventError { .. } => ErrorCode::WorkflowFailed,
                WorkflowError::QueryError { .. } => ErrorCode::WorkflowFailed,
                WorkflowError::InvalidState(_) => ErrorCode::WorkflowStateError,
                WorkflowError::NotFound { .. } => ErrorCode::WorkflowNotFound,
                WorkflowError::ReplayError(_) => ErrorCode::WorkflowReplayError,
                WorkflowError::RestartFreshError(_) => ErrorCode::WorkflowFailed,
                WorkflowError::VersionMismatch { .. } => ErrorCode::WorkflowVersionMismatch,
                WorkflowError::Suspended { .. } => ErrorCode::Internal, // not a real error
            },
            Error::Task(t) => match t {
                TaskError::ExecutionFailed(_) => ErrorCode::TaskFailed,
                TaskError::Timeout { .. } => ErrorCode::TaskTimeout,
                TaskError::Canceled => ErrorCode::TaskCanceled,
                TaskError::HeartbeatFailed(_) => ErrorCode::TaskHeartbeatFailed,
                TaskError::HeartbeatTimeout { .. } => ErrorCode::TaskHeartbeatFailed,
                TaskError::NotFound { .. } => ErrorCode::TaskNotFound,
                TaskError::AlreadyCompleted { .. } => ErrorCode::TaskFailed,
                TaskError::Panic(_) => ErrorCode::TaskFailed,
                TaskError::RetryLimitExceeded { .. } => ErrorCode::TaskRetryLimitExceeded,
                TaskError::InvalidInput(_) => ErrorCode::TaskInvalidInput,
                TaskError::InvalidConfiguration(_) => ErrorCode::InvalidConfiguration,
                TaskError::Application { .. } => ErrorCode::TaskFailed,
            },
            Error::Client(c) => match c {
                ClientError::ConnectionFailed { .. } => ErrorCode::ConnectionFailed,
                ClientError::RequestFailed(_) => ErrorCode::ServerError,
                ClientError::Timeout { .. } => ErrorCode::RequestTimeout,
                ClientError::InvalidUrl(_) => ErrorCode::InvalidRequest,
                ClientError::AuthenticationFailed(_) => ErrorCode::AuthenticationFailed,
                ClientError::AuthorizationFailed(_) => ErrorCode::AuthorizationFailed,
                ClientError::WorkflowNotFound { .. } => ErrorCode::WorkflowNotFound,
                ClientError::WorkflowAlreadyExists { .. } => ErrorCode::WorkflowAlreadyExists,
                ClientError::InvalidInput(_) => ErrorCode::InvalidRequest,
                ClientError::InvalidResponse(_) => ErrorCode::ServerError,
                ClientError::ServerError { .. } => ErrorCode::ServerError,
                ClientError::Serialization(_) => ErrorCode::SerializationError,
                ClientError::Deserialization(_) => ErrorCode::SerializationError,
            },
            Error::Worker(s) => match s {
                WorkerError::InvalidConfiguration(_) => ErrorCode::InvalidConfiguration,
                WorkerError::StartupFailed(_) => ErrorCode::StartupFailed,
                WorkerError::AlreadyRunning => ErrorCode::InvalidConfiguration,
                WorkerError::NotRunning => ErrorCode::InvalidConfiguration,
                WorkerError::ConnectionFailed { .. } => ErrorCode::ConnectionFailed,
                WorkerError::PollingError(_) => ErrorCode::PollingError,
                WorkerError::RegistrationError(_) => ErrorCode::StartupFailed,
                WorkerError::HandlerNotFound { .. } => ErrorCode::HandlerNotFound,
                WorkerError::HandlerExecutionError(_) => ErrorCode::Internal,
                WorkerError::ShutdownError(_) => ErrorCode::ShutdownError,
                WorkerError::ShutdownTimeout { .. } => ErrorCode::ShutdownError,
                WorkerError::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
                WorkerError::TaskQueueNotFound { .. } => ErrorCode::HandlerNotFound,
            },
            Error::Actor(_) => ErrorCode::Internal,
            Error::Network(_) => ErrorCode::NetworkError,
            Error::Configuration(_) => ErrorCode::InvalidConfiguration,
            Error::Serialization(_) => ErrorCode::SerializationError,
            Error::Timeout(_) => ErrorCode::RequestTimeout,
            Error::Io(_) => ErrorCode::IoError,
            Error::Other(_) => ErrorCode::Internal,
        }
    }

    /// The [`Severity`] of this error, the same in every SDK.
    pub fn severity(&self) -> Severity {
        match self {
            // Critical: service cannot function
            Error::Worker(WorkerError::StartupFailed(_)) => Severity::Critical,
            Error::Worker(WorkerError::ConnectionFailed { .. }) => Severity::Critical,
            Error::Configuration(_) => Severity::Critical,

            // High: significant impact, needs attention
            Error::Workflow(WorkflowError::NonDeterministic(_)) => Severity::High,
            Error::Workflow(WorkflowError::VersionMismatch { .. }) => Severity::High,
            Error::Workflow(WorkflowError::Panic(_)) => Severity::High,
            Error::Task(TaskError::Panic(_)) => Severity::High,
            Error::Task(TaskError::RetryLimitExceeded { .. }) => Severity::High,
            Error::Client(ClientError::AuthenticationFailed(_)) => Severity::High,
            Error::Client(ClientError::AuthorizationFailed(_)) => Severity::High,
            Error::Worker(WorkerError::ShutdownError(_)) => Severity::High,
            Error::Worker(WorkerError::ShutdownTimeout { .. }) => Severity::High,
            Error::Worker(WorkerError::ResourceExhausted { .. }) => Severity::High,

            // Medium: degraded functionality
            Error::Workflow(WorkflowError::TaskFailed { .. }) => Severity::Medium,
            Error::Workflow(WorkflowError::ChildWorkflowFailed { .. }) => Severity::Medium,
            Error::Workflow(WorkflowError::Timeout { .. }) => Severity::Medium,
            Error::Task(TaskError::ExecutionFailed(_)) => Severity::Medium,
            Error::Task(TaskError::Timeout { .. }) => Severity::Medium,
            Error::Task(TaskError::HeartbeatFailed(_)) => Severity::Medium,
            Error::Task(TaskError::HeartbeatTimeout { .. }) => Severity::Medium,
            Error::Client(ClientError::ConnectionFailed { .. }) => Severity::Medium,
            Error::Client(ClientError::Timeout { .. }) => Severity::Medium,
            Error::Client(ClientError::ServerError { .. }) => Severity::Medium,
            Error::Worker(WorkerError::PollingError(_)) => Severity::Medium,
            Error::Network(_) => Severity::Medium,
            Error::Io(_) => Severity::Medium,

            // Low: expected or informational
            Error::Workflow(WorkflowError::Canceled) => Severity::Low,
            Error::Workflow(WorkflowError::Suspended { .. }) => Severity::Low,
            Error::Workflow(WorkflowError::NotFound { .. }) => Severity::Low,
            Error::Task(TaskError::Canceled) => Severity::Low,
            Error::Task(TaskError::NotFound { .. }) => Severity::Low,
            Error::Client(ClientError::WorkflowNotFound { .. }) => Severity::Low,
            Error::Client(ClientError::WorkflowAlreadyExists { .. }) => Severity::Low,
            Error::Serialization(_) => Severity::Low,

            // Default to Medium for uncovered cases
            _ => Severity::Medium,
        }
    }
}

// ============================================================================
// Convenience Constructors
// ============================================================================

impl WorkflowError {
    /// Create a task failed error
    pub fn task_failed(
        task_type: impl Into<String>,
        attempts: u32,
        reason: impl Into<String>,
    ) -> Self {
        Self::TaskFailed {
            task_type: task_type.into(),
            attempts,
            reason: reason.into(),
        }
    }

    /// Create a timer failed error
    pub fn timer_failed(timer_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::TimerFailed {
            timer_id: timer_id.into(),
            reason: reason.into(),
        }
    }

    /// Create a child workflow failed error
    pub fn child_workflow_failed(
        workflow_type: impl Into<String>,
        workflow_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::ChildWorkflowFailed {
            workflow_type: workflow_type.into(),
            workflow_id: workflow_id.into(),
            reason: reason.into(),
        }
    }

    /// Create an event error
    pub fn event_error(event: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::EventError {
            event: event.into(),
            reason: reason.into(),
        }
    }

    /// Create a query error
    pub fn query_error(query: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::QueryError {
            query: query.into(),
            reason: reason.into(),
        }
    }

    /// Create a not found error
    pub fn not_found(workflow_id: impl Into<String>) -> Self {
        Self::NotFound {
            workflow_id: workflow_id.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout(timeout: std::time::Duration) -> Self {
        Self::Timeout { timeout }
    }

    /// Create a non-deterministic error
    pub fn non_deterministic(reason: impl Into<String>) -> Self {
        Self::NonDeterministic(reason.into())
    }

    /// Create a version mismatch error
    pub fn version_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::VersionMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create the suspension signal: the workflow waits on these operations
    pub fn suspended(reason: impl Into<String>, pending_operations: Vec<String>) -> Self {
        Self::Suspended {
            reason: reason.into(),
            pending_operations,
        }
    }
}

impl TaskError {
    /// Create an execution failed error
    pub fn execution_failed(reason: impl Into<String>) -> Self {
        Self::ExecutionFailed(reason.into())
    }

    /// Create a heartbeat failed error
    pub fn heartbeat_failed(reason: impl Into<String>) -> Self {
        Self::HeartbeatFailed(reason.into())
    }

    /// Create a timeout error
    pub fn timeout(timeout: std::time::Duration) -> Self {
        Self::Timeout { timeout }
    }

    /// Create a not found error
    pub fn not_found(task_id: impl Into<String>) -> Self {
        Self::NotFound {
            task_id: task_id.into(),
        }
    }

    /// Create a retry limit exceeded error
    pub fn retry_limit_exceeded(attempts: u32) -> Self {
        Self::RetryLimitExceeded { attempts }
    }

    /// Create a heartbeat timeout error
    pub fn heartbeat_timeout(timeout: std::time::Duration) -> Self {
        Self::HeartbeatTimeout { timeout }
    }

    /// Create a task already completed error
    pub fn already_completed(task_id: impl Into<String>) -> Self {
        Self::AlreadyCompleted {
            task_id: task_id.into(),
        }
    }

    /// Create an invalid input error
    pub fn invalid_input(reason: impl Into<String>) -> Self {
        Self::InvalidInput(reason.into())
    }

    /// A failure of type `error_type`, retried unless the task's retry policy
    /// lists that type as non-retryable.
    pub fn application(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Application {
            error_type: error_type.into(),
            message: message.into(),
            non_retryable: false,
        }
    }

    /// A failure of type `error_type` that is never retried, whatever the
    /// task's retry policy allows.
    pub fn non_retryable(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Application {
            error_type: error_type.into(),
            message: message.into(),
            non_retryable: true,
        }
    }
}

impl ClientError {
    /// Create a connection failed error
    pub fn connection_failed(url: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConnectionFailed {
            url: url.into(),
            reason: reason.into(),
        }
    }

    /// Create a server error
    pub fn server_error(status: u16, message: impl Into<String>) -> Self {
        Self::ServerError {
            status,
            message: message.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout(timeout: std::time::Duration) -> Self {
        Self::Timeout { timeout }
    }

    /// Create a workflow not found error
    pub fn workflow_not_found(workflow_id: impl Into<String>) -> Self {
        Self::WorkflowNotFound {
            workflow_id: workflow_id.into(),
        }
    }

    /// Create a workflow already exists error
    pub fn workflow_already_exists(workflow_id: impl Into<String>) -> Self {
        Self::WorkflowAlreadyExists {
            workflow_id: workflow_id.into(),
            run_id: None,
        }
    }

    /// Create a workflow already exists error naming the run in the way
    pub fn workflow_already_exists_with_run(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self::WorkflowAlreadyExists {
            workflow_id: workflow_id.into(),
            run_id: Some(run_id.into()),
        }
    }

    /// Create a request failed error
    pub fn request_failed(reason: impl Into<String>) -> Self {
        Self::RequestFailed(reason.into())
    }

    /// Create a serialization error
    pub fn serialization(reason: impl Into<String>) -> Self {
        Self::Serialization(reason.into())
    }

    /// Create a deserialization error
    pub fn deserialization(reason: impl Into<String>) -> Self {
        Self::Deserialization(reason.into())
    }
}

impl WorkerError {
    /// Create a connection failed error
    pub fn connection_failed(url: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConnectionFailed {
            url: url.into(),
            reason: reason.into(),
        }
    }

    /// Create a handler not found error
    pub fn handler_not_found(handler_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self::HandlerNotFound {
            handler_type: handler_type.into(),
            name: name.into(),
        }
    }

    /// Create an invalid configuration error
    pub fn invalid_configuration(reason: impl Into<String>) -> Self {
        Self::InvalidConfiguration(reason.into())
    }

    /// Create a startup failed error
    pub fn startup_failed(reason: impl Into<String>) -> Self {
        Self::StartupFailed(reason.into())
    }

    /// Create a polling error
    pub fn polling_error(reason: impl Into<String>) -> Self {
        Self::PollingError(reason.into())
    }

    /// Create a resource exhausted error
    pub fn resource_exhausted(resource: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
        }
    }

    /// Create a shutdown timeout error
    pub fn shutdown_timeout(timeout: std::time::Duration) -> Self {
        Self::ShutdownTimeout { timeout }
    }

    /// Create a task queue not found error
    pub fn task_queue_not_found(queue: impl Into<String>) -> Self {
        Self::TaskQueueNotFound {
            queue: queue.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_suspend_only_true_for_suspended() {
        let suspended = Error::Workflow(WorkflowError::Suspended {
            reason: "Task 'charge' scheduled for execution".to_string(),
            pending_operations: vec!["charge_0".to_string()],
        });
        assert!(suspended.is_suspend());

        // A real task failure is not a suspend. This distinction keeps sagas
        // from compensating healthy workflows.
        let failed = Error::Workflow(WorkflowError::task_failed("charge", 3, "declined"));
        assert!(!failed.is_suspend());

        let other = Error::Serialization("bad json".to_string());
        assert!(!other.is_suspend());
    }

    #[test]
    fn test_workflow_error_display() {
        let err = WorkflowError::task_failed("ProcessPayment", 3, "Network timeout");
        assert_eq!(
            err.to_string(),
            "Task 'ProcessPayment' failed after 3 attempts: Network timeout"
        );
    }

    #[test]
    fn test_task_error_display() {
        let err = TaskError::execution_failed("Database connection lost");
        assert_eq!(
            err.to_string(),
            "Task execution failed: Database connection lost"
        );
    }

    #[test]
    fn test_client_error_display() {
        let err = ClientError::server_error(500, "Internal server error");
        assert_eq!(
            err.to_string(),
            "Server error (status 500): Internal server error"
        );
    }

    #[test]
    fn test_service_error_display() {
        let err = WorkerError::handler_not_found("workflow", "OrderWorkflow");
        assert_eq!(
            err.to_string(),
            "No handler registered for 'workflow' with name 'OrderWorkflow'"
        );
    }

    #[test]
    fn test_error_conversion() {
        let workflow_err = WorkflowError::Canceled;
        let err: Error = workflow_err.into();
        assert!(matches!(err, Error::Workflow(_)));
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert!(ok_result.is_ok());

        let err_result: Result<i32> = Err(Error::Configuration("Invalid config".to_string()));
        assert!(err_result.is_err());
    }

    #[test]
    fn test_error_is_retryable() {
        // Retryable errors
        let err = Error::Client(ClientError::ConnectionFailed {
            url: "http://test".to_string(),
            reason: "timeout".to_string(),
        });
        assert!(err.is_retryable());

        let err = Error::Client(ClientError::Timeout {
            timeout: std::time::Duration::from_secs(30),
        });
        assert!(err.is_retryable());

        let err = Error::Client(ClientError::ServerError {
            status: 503,
            message: "Service unavailable".to_string(),
        });
        assert!(err.is_retryable());

        // Non-retryable errors
        let err = Error::Configuration("Invalid config".to_string());
        assert!(!err.is_retryable());

        let err = Error::Client(ClientError::ServerError {
            status: 400,
            message: "Bad request".to_string(),
        });
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_is_permanent() {
        // Permanent errors
        let err = Error::Configuration("Invalid config".to_string());
        assert!(err.is_permanent());

        let err = Error::Client(ClientError::AuthenticationFailed(
            "Invalid token".to_string(),
        ));
        assert!(err.is_permanent());

        let err = Error::Workflow(WorkflowError::NonDeterministic(
            "Replay mismatch".to_string(),
        ));
        assert!(err.is_permanent());

        let err = Error::Client(ClientError::ServerError {
            status: 404,
            message: "Not found".to_string(),
        });
        assert!(err.is_permanent());

        // Non-permanent errors
        let err = Error::Client(ClientError::Timeout {
            timeout: std::time::Duration::from_secs(30),
        });
        assert!(!err.is_permanent());
    }

    #[test]
    fn test_error_with_context() {
        let err = Error::Configuration("Invalid port".to_string());
        let err_with_ctx = err.with_context("Failed to start server");
        assert!(err_with_ctx.to_string().contains("Failed to start server"));
        assert!(err_with_ctx.to_string().contains("Invalid port"));
    }

    #[test]
    fn test_workflow_error_constructors() {
        let err = WorkflowError::not_found("wf-123");
        assert!(matches!(err, WorkflowError::NotFound { .. }));

        let err = WorkflowError::timeout(std::time::Duration::from_secs(30));
        assert!(matches!(err, WorkflowError::Timeout { .. }));

        let err = WorkflowError::non_deterministic("mismatch");
        assert!(matches!(err, WorkflowError::NonDeterministic(_)));
    }

    #[test]
    fn test_task_error_constructors() {
        let err = TaskError::not_found("task-456");
        assert!(matches!(err, TaskError::NotFound { .. }));

        let err = TaskError::timeout(std::time::Duration::from_secs(10));
        assert!(matches!(err, TaskError::Timeout { .. }));

        let err = TaskError::retry_limit_exceeded(5);
        assert!(matches!(err, TaskError::RetryLimitExceeded { attempts: 5 }));
    }

    /// A refused start names the run holding the id; losing it on the way up
    /// leaves a caller that meant to start the workflow once unable to reach
    /// the run already doing the work.
    #[test]
    fn a_refused_start_keeps_the_run_in_the_way() {
        let err: Error =
            orcher_sdk_core::error::Error::workflow_already_exists_with_run("order-7", "run-7")
                .into();
        assert_eq!(err.code(), ErrorCode::WorkflowAlreadyExists);
        match err {
            Error::Client(ClientError::WorkflowAlreadyExists {
                workflow_id,
                run_id,
            }) => {
                assert_eq!(workflow_id, "order-7");
                assert_eq!(run_id.as_deref(), Some("run-7"));
            }
            other => panic!("expected WorkflowAlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn test_client_error_constructors() {
        let err = ClientError::workflow_not_found("wf-789");
        assert!(matches!(err, ClientError::WorkflowNotFound { .. }));

        let err = ClientError::workflow_already_exists("wf-789");
        assert!(matches!(err, ClientError::WorkflowAlreadyExists { .. }));

        let err = ClientError::request_failed("network error");
        assert!(matches!(err, ClientError::RequestFailed(_)));
    }

    #[test]
    fn test_service_error_constructors() {
        let err = WorkerError::polling_error("connection lost");
        assert!(matches!(err, WorkerError::PollingError(_)));

        let err = WorkerError::resource_exhausted("memory");
        assert!(matches!(err, WorkerError::ResourceExhausted { .. }));

        let err = WorkerError::task_queue_not_found("orders");
        assert!(matches!(err, WorkerError::TaskQueueNotFound { .. }));
    }

    #[test]
    fn test_sdk_core_error_conversion() {
        use orcher_sdk_core::error::Error as CoreError;

        // Workflow errors
        let core_err = CoreError::workflow_not_found("wf-123");
        let sdk_err: Error = core_err.into();
        assert!(matches!(
            sdk_err,
            Error::Workflow(WorkflowError::NotFound { .. })
        ));

        // Task errors
        let core_err = CoreError::task_execution_failed("task-456", "failed");
        let sdk_err: Error = core_err.into();
        assert!(matches!(
            sdk_err,
            Error::Task(TaskError::ExecutionFailed(_))
        ));

        // Connection errors
        let core_err = CoreError::connection("timeout");
        let sdk_err: Error = core_err.into();
        assert!(matches!(
            sdk_err,
            Error::Client(ClientError::ConnectionFailed { .. })
        ));

        // Determinism errors
        let core_err = CoreError::determinism_violation("replay mismatch");
        let sdk_err: Error = core_err.into();
        assert!(matches!(
            sdk_err,
            Error::Workflow(WorkflowError::NonDeterministic(_))
        ));
    }
}

#[cfg(test)]
mod typed_client_error_tests {
    use super::*;
    use orcher_sdk_core::error::Error as CoreError;

    /// A missing workflow keeps its type, so a caller can tell "this workflow
    /// does not exist" from "the server is down".
    #[test]
    fn a_missing_workflow_maps_to_a_typed_not_found() {
        let err: Error = CoreError::workflow_not_found("wf-1").into();

        assert!(
            matches!(err, Error::Workflow(WorkflowError::NotFound { .. })),
            "expected a typed NotFound, got {err:?}"
        );
    }

    /// A workflow that will never exist must not be retried, or a retry loop
    /// keeps hitting the server for nothing.
    #[test]
    fn a_missing_workflow_is_not_retryable() {
        let err: Error = CoreError::workflow_not_found("wf-1").into();

        assert!(!err.is_retryable(), "a missing workflow was retried");
    }

    /// Transient gRPC codes map into the HTTP retryable range, so they are
    /// retried.
    #[test]
    fn transient_grpc_failures_are_retryable() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::ResourceExhausted,
        ] {
            let err: Error = CoreError::GrpcStatus(tonic::Status::new(code, "boom")).into();
            assert!(
                err.is_retryable(),
                "{code:?} should be retryable but was classified permanent"
            );
        }
    }

    /// Permanent gRPC codes are not retried.
    #[test]
    fn permanent_grpc_failures_are_not_retryable() {
        for code in [
            tonic::Code::NotFound,
            tonic::Code::InvalidArgument,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::AlreadyExists,
        ] {
            let err: Error = CoreError::GrpcStatus(tonic::Status::new(code, "nope")).into();
            assert!(
                !err.is_retryable(),
                "{code:?} should be permanent but was classified retryable"
            );
        }
    }

    /// The caller sees an HTTP status, not a raw gRPC ordinal that looks like
    /// an HTTP code and is not one.
    #[test]
    fn grpc_status_is_translated_for_the_caller() {
        let err: Error =
            CoreError::GrpcStatus(tonic::Status::new(tonic::Code::NotFound, "missing")).into();
        match err {
            Error::Client(ClientError::ServerError { status, .. }) => assert_eq!(status, 404),
            other => panic!("expected ServerError, got {other:?}"),
        }
    }
}
