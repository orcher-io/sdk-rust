//! Handler types and auto-registration entries for tasks, workflows, updates
//! and actors.
//!
//! With the `auto-register` feature, the `#[task]`, `#[workflow]` and `#[update]`
//! macros submit a registration entry to the `inventory` crate for each handler.
//! [`WorkerBuilder::new`](crate::worker::WorkerBuilder::new) calls
//! `Registry::collect_from_global_registry`, which runs every submitted
//! registration function against the worker's registry. Actor operations take
//! a separate path: the `#[operations]` macro adds them to the global registry
//! from constructor functions that run before `main`.
//!
//! ## Example
//!
//! ```rust,no_run
//! use orcher_sdk::prelude::*;
//!
//! #[task] // Submits a registration entry to `inventory`.
//! async fn greet(_ctx: TaskContext, name: String) -> Result<String> {
//!     Ok(format!("Hello, {name}"))
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // The builder collects every submitted entry.
//!     let worker = Worker::builder()
//!         .server_url("http://localhost:50051")
//!         .build()
//!         .await?;
//!     worker.run().await
//! }
//! ```

use crate::error::Error;
use crate::payload::Payload;
use crate::task::RetryPolicy;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Type-erased task handler: takes the task context and input payload and
/// resolves to the output payload.
pub type TaskHandlerFn = Arc<
    dyn Fn(crate::TaskContext, Payload) -> BoxFuture<'static, Result<Payload, Error>> + Send + Sync,
>;

/// Type-erased workflow handler: takes the workflow context and input payload
/// and resolves to the output payload.
pub type WorkflowHandlerFn = Arc<
    dyn Fn(crate::WorkflowContext, Payload) -> BoxFuture<'static, Result<Payload, Error>>
        + Send
        + Sync,
>;

/// Type-erased update handler: takes the workflow context and input payload and
/// resolves to the output payload.
///
/// The context is a clone that shares state with the running workflow through
/// an `Arc`, so the handler can read and modify that workflow's state.
pub type UpdateHandlerFn = Arc<
    dyn Fn(crate::WorkflowContext, Payload) -> BoxFuture<'static, Result<Payload, Error>>
        + Send
        + Sync,
>;

/// Type-erased actor operation handler: takes the actor context and input
/// bytes and resolves to the output bytes.
pub type ActorHandlerFn = Arc<
    dyn Fn(crate::actor::ActorContext, Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, Error>>
        + Send
        + Sync,
>;

/// Function that adds one task handler to a [`Registry`](crate::worker::Registry).
#[cfg(feature = "auto-register")]
pub type TaskRegistrationFn = fn(&mut crate::worker::Registry);

/// Task registration entry collected through `inventory`.
///
/// The `#[task]` macro submits one per task. Its function is run against the
/// worker's registry when the [`WorkerBuilder`](crate::worker::WorkerBuilder)
/// is created.
///
/// ## Example of generated code
///
/// ```text
/// fn register_my_task(registry: &mut Registry) {
///     let handler = Arc::new(|ctx, input| {
///         Box::pin(async move {
///             my_task_handler(ctx, input).await
///         })
///     });
///     registry.register_task("my_task", TaskHandler {
///         name: "my_task".to_string(),
///         handler: Some(handler),
///     });
/// }
///
/// ::orcher_sdk::__private::inventory::submit! {
///     TaskRegistration::new(register_my_task)
/// }
/// ```
#[cfg(feature = "auto-register")]
#[derive(Clone, Copy)]
pub struct TaskRegistration {
    /// Function that registers the handler.
    pub register_fn: TaskRegistrationFn,
}

#[cfg(feature = "auto-register")]
inventory::collect!(TaskRegistration);

#[cfg(feature = "auto-register")]
impl TaskRegistration {
    /// Creates an entry; `const` so it can be used in `inventory::submit!`.
    pub const fn new(register_fn: TaskRegistrationFn) -> Self {
        Self { register_fn }
    }

    /// Runs the registration function against `registry`.
    pub fn register(&self, registry: &mut crate::worker::Registry) {
        (self.register_fn)(registry);
    }
}

impl std::fmt::Debug for TaskRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistration")
            .field("register_fn", &"<function>")
            .finish()
    }
}

/// Function that adds one workflow handler to a [`Registry`](crate::worker::Registry).
#[cfg(feature = "auto-register")]
pub type WorkflowRegistrationFn = fn(&mut crate::worker::Registry);

/// Workflow registration entry collected through `inventory`.
///
/// The `#[workflow]` macro submits one per workflow. Its function is run
/// against the worker's registry when the
/// [`WorkerBuilder`](crate::worker::WorkerBuilder) is created.
///
/// ## Example of generated code
///
/// ```text
/// fn register_order_workflow(registry: &mut Registry) {
///     let handler = Arc::new(|ctx, input| {
///         Box::pin(async move {
///             order_workflow_handler(ctx, input).await
///         })
///     });
///     registry.register_workflow("order_workflow", WorkflowHandler {
///         name: "order_workflow".to_string(),
///         handler: Some(handler),
///     });
/// }
///
/// ::orcher_sdk::__private::inventory::submit! {
///     WorkflowRegistration::new(register_order_workflow)
/// }
/// ```
#[cfg(feature = "auto-register")]
#[derive(Clone, Copy)]
pub struct WorkflowRegistration {
    /// Function that registers the handler.
    pub register_fn: WorkflowRegistrationFn,
}

#[cfg(feature = "auto-register")]
inventory::collect!(WorkflowRegistration);

#[cfg(feature = "auto-register")]
impl WorkflowRegistration {
    /// Creates an entry; `const` so it can be used in `inventory::submit!`.
    pub const fn new(register_fn: WorkflowRegistrationFn) -> Self {
        Self { register_fn }
    }

    /// Runs the registration function against `registry`.
    pub fn register(&self, registry: &mut crate::worker::Registry) {
        (self.register_fn)(registry);
    }
}

impl std::fmt::Debug for WorkflowRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowRegistration")
            .field("register_fn", &"<function>")
            .finish()
    }
}

/// Function that adds one update handler to a [`Registry`](crate::worker::Registry).
#[cfg(feature = "auto-register")]
pub type UpdateRegistrationFn = fn(&mut crate::worker::Registry);

/// Update handler registration entry collected through `inventory`.
///
/// The `#[update]` macro submits one per update handler. Its function is run
/// against the worker's registry when the
/// [`WorkerBuilder`](crate::worker::WorkerBuilder) is created.
///
/// ## Example of generated code
///
/// ```text
/// fn register_change_address(registry: &mut Registry) {
///     let handler = Arc::new(|ctx, input| {
///         Box::pin(async move {
///             change_address_handler(ctx, input).await
///         })
///     });
///     registry.updates.insert("change_address".to_string(), UpdateHandler { ... });
/// }
///
/// ::orcher_sdk::__private::inventory::submit! {
///     UpdateRegistration::new(register_change_address)
/// }
/// ```
#[cfg(feature = "auto-register")]
#[derive(Clone, Copy)]
pub struct UpdateRegistration {
    /// Function that registers the handler.
    pub register_fn: UpdateRegistrationFn,
}

#[cfg(feature = "auto-register")]
inventory::collect!(UpdateRegistration);

#[cfg(feature = "auto-register")]
impl UpdateRegistration {
    /// Creates an entry; `const` so it can be used in `inventory::submit!`.
    pub const fn new(register_fn: UpdateRegistrationFn) -> Self {
        Self { register_fn }
    }

    /// Runs the registration function against `registry`.
    pub fn register(&self, registry: &mut crate::worker::Registry) {
        (self.register_fn)(registry);
    }
}

impl std::fmt::Debug for UpdateRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateRegistration")
            .field("register_fn", &"<function>")
            .finish()
    }
}

/// Registration entry for one actor operation handler.
///
/// The `#[operations]` macro adds one entry per operation to the global
/// registry from a constructor function that runs before `main`. The
/// [`WorkerBuilder`](crate::worker::WorkerBuilder) copies them into the
/// worker's registry.
///
/// ## Example
///
/// ```text
/// // Generated by the #[operations] macro:
/// #[::orcher_sdk::ctor::ctor]
/// fn __orcher_register_actor_handler_ShoppingCart_add_item() {
///     ::orcher_sdk::worker::registry::GLOBAL_REGISTRY
///         .write()
///         .unwrap()
///         .register_actor_handler_direct(
///             ActorHandlerRegistration {
///                 actor_name: "ShoppingCart",
///                 operation_name: "add_item",
///                 mode: ::orcher_sdk::actor::types::OperationMode::Exclusive,
///                 handler: Arc::new(|ctx, payload| {
///                     Box::pin(async move {
///                         // ...
///                     })
///                 }),
///             }
///         );
/// }
/// ```
#[derive(Clone)]
pub struct ActorHandlerRegistration {
    /// Actor type name, such as `"ShoppingCart"`.
    pub actor_name: &'static str,

    /// Operation name, such as `"add_item"`.
    pub operation_name: &'static str,

    /// Whether the operation runs exclusively or may share the actor.
    pub mode: crate::actor::types::OperationMode,

    /// The handler to invoke.
    pub handler: ActorHandlerFn,
}

impl std::fmt::Debug for ActorHandlerRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorHandlerRegistration")
            .field("actor_name", &self.actor_name)
            .field("operation_name", &self.operation_name)
            .field("mode", &self.mode)
            .field("handler", &"<function>")
            .finish()
    }
}

/// Configuration and documentation declared for a task.
#[derive(Clone, Debug)]
pub struct TaskMetadata {
    /// Human-readable description.
    pub description: Option<String>,

    /// Task timeout, in seconds.
    pub timeout_seconds: Option<u64>,

    /// How long the task may go without a heartbeat before it is considered
    /// stalled, in seconds.
    pub heartbeat_timeout_seconds: Option<u64>,

    /// Retry policy.
    pub retry_policy: Option<RetryPolicy>,

    /// Namespace.
    pub namespace: Option<String>,

    /// Task version.
    pub version: Option<String>,

    /// Task queue the task is routed to.
    pub task_queue: Option<String>,

    /// Priority from 0 to 100; higher runs first.
    pub priority: Option<u8>,

    /// Maximum concurrent executions of this task.
    pub max_concurrent: Option<u32>,

    /// Maximum executions per second.
    pub rate_limit: Option<u32>,

    /// Template for the idempotency key that deduplicates executions.
    pub idempotency_key: Option<String>,
}

/// Configuration and documentation declared for a workflow.
#[derive(Clone, Debug)]
pub struct WorkflowMetadata {
    /// Human-readable description.
    pub description: Option<String>,

    /// Workflow version; semantic versioning is recommended.
    pub version: String,

    /// Workflow timeout, in seconds.
    pub timeout_seconds: Option<u64>,

    /// Tags for categorizing and filtering.
    pub tags: Vec<String>,

    /// Namespace.
    pub namespace: Option<String>,

    /// Task queue the workflow runs on.
    pub task_queue: Option<String>,

    /// Maximum steps of one execution that run concurrently.
    pub max_concurrent_steps: Option<u32>,

    /// Maximum concurrent executions of this workflow.
    pub max_concurrent_executions: Option<u32>,

    /// Cron expression, for periodic workflows.
    pub cron_schedule: Option<String>,

    /// Whether heartbeats are sent automatically.
    pub auto_heartbeat: bool,

    /// Interval between automatic heartbeats, in seconds.
    pub heartbeat_interval_seconds: Option<u64>,

    /// What to do on timeout: `"cancel"`, `"cancel_with_notification"` or `"fail"`.
    pub on_timeout: Option<String>,

    /// Retry policy for the whole workflow.
    pub retry_policy: Option<RetryPolicy>,
}

// Task and workflow metadata and the task retry registry all store
// `crate::task::RetryPolicy`; there is no separate registration-time retry type.

// ============================================================================
// Task retry-policy registry
// ============================================================================

/// Process-wide map from task name to its declared retry policy.
///
/// `#[task(retry = N)]` (or the full `retry_policy(...)` form) belongs to the task
/// definition, but a workflow schedules a task by name, so the policy has to be
/// looked up when the task is scheduled. The `#[task]` macro registers the policy
/// here, and scheduling reads it into the schedule command. Tasks without an
/// explicit policy are absent and get the default.
static TASK_RETRY_POLICIES: std::sync::OnceLock<
    std::sync::RwLock<std::collections::HashMap<String, RetryPolicy>>,
> = std::sync::OnceLock::new();

fn task_retry_registry(
) -> &'static std::sync::RwLock<std::collections::HashMap<String, RetryPolicy>> {
    TASK_RETRY_POLICIES.get_or_init(|| std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Limits a task declared on its `#[task]` attribute.
///
/// An absent field means the task declared no limit, which differs from declaring
/// zero: the scheduler applies its own default instead of giving the task no time
/// at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeclaredTaskLimits {
    /// How long the task may run once started, in seconds.
    pub timeout_secs: Option<u64>,
    /// How long the task may go between heartbeats, in seconds.
    pub heartbeat_timeout_secs: Option<u64>,
}

/// Limits declared on `#[task]`, keyed by task name. Tasks that declare none are
/// absent.
static TASK_LIMITS: std::sync::OnceLock<
    std::sync::RwLock<std::collections::HashMap<String, DeclaredTaskLimits>>,
> = std::sync::OnceLock::new();

fn task_limits_registry(
) -> &'static std::sync::RwLock<std::collections::HashMap<String, DeclaredTaskLimits>> {
    TASK_LIMITS.get_or_init(|| std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Records the limits a task declared, so that scheduling applies them.
///
/// Called by the `#[task]` macro at registration time. Without this, a task
/// declared with `#[task(timeout = 77)]` would be scheduled with the default
/// timeout. Declaring no limits is a no-op.
pub fn register_task_limits(name: impl Into<String>, limits: DeclaredTaskLimits) {
    if limits.timeout_secs.is_none() && limits.heartbeat_timeout_secs.is_none() {
        return;
    }
    if let Ok(mut map) = task_limits_registry().write() {
        map.insert(name.into(), limits);
    }
}

/// Returns the limits a task declared, or `None` if it declared none.
pub fn task_limits(name: &str) -> Option<DeclaredTaskLimits> {
    task_limits_registry().read().ok()?.get(name).copied()
}

/// Records a task's retry policy so that scheduling attaches it to the task.
///
/// Called by the `#[task]` macro at registration time. A `None` policy is a no-op,
/// leaving the task on the default retry policy.
pub fn register_task_retry_policy(name: impl Into<String>, policy: Option<RetryPolicy>) {
    if let Some(policy) = policy {
        if let Ok(mut map) = task_retry_registry().write() {
            map.insert(name.into(), policy);
        }
    }
}

/// Returns the retry policy registered for a task, or `None` if it declared none
/// (the caller then uses the default).
pub fn task_retry_policy(name: &str) -> Option<RetryPolicy> {
    task_retry_registry().read().ok()?.get(name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_interval, std::time::Duration::from_secs(1));
        assert_eq!(policy.max_interval, std::time::Duration::from_secs(60));
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_task_metadata() {
        let metadata = TaskMetadata {
            description: Some("Test task".to_string()),
            timeout_seconds: Some(120),
            heartbeat_timeout_seconds: Some(30),
            retry_policy: Some(RetryPolicy::default()),
            namespace: Some("default".to_string()),
            version: Some("1.0.0".to_string()),
            task_queue: Some("workers".to_string()),
            priority: Some(50),
            max_concurrent: Some(10),
            rate_limit: Some(100),
            idempotency_key: Some("task-{id}".to_string()),
        };

        assert_eq!(metadata.description, Some("Test task".to_string()));
        assert_eq!(metadata.timeout_seconds, Some(120));
        assert_eq!(metadata.heartbeat_timeout_seconds, Some(30));
        assert!(metadata.retry_policy.is_some());
        assert_eq!(metadata.task_queue, Some("workers".to_string()));
        assert_eq!(metadata.priority, Some(50));
        assert_eq!(metadata.max_concurrent, Some(10));
        assert_eq!(metadata.rate_limit, Some(100));
    }

    #[test]
    fn test_workflow_metadata() {
        let metadata = WorkflowMetadata {
            description: Some("Test workflow".to_string()),
            version: "1.0.0".to_string(),
            timeout_seconds: Some(600),
            tags: vec!["test".to_string(), "demo".to_string()],
            namespace: Some("default".to_string()),
            task_queue: Some("orders".to_string()),
            max_concurrent_steps: Some(10),
            max_concurrent_executions: Some(5),
            cron_schedule: Some("0 0 * * *".to_string()),
            auto_heartbeat: true,
            heartbeat_interval_seconds: Some(30),
            on_timeout: Some("cancel".to_string()),
            retry_policy: Some(RetryPolicy::default()),
        };

        assert_eq!(metadata.version, "1.0.0");
        assert_eq!(metadata.tags.len(), 2);
        assert!(metadata.tags.contains(&"test".to_string()));
        assert_eq!(metadata.task_queue, Some("orders".to_string()));
        assert_eq!(metadata.max_concurrent_executions, Some(5));
        assert_eq!(metadata.cron_schedule, Some("0 0 * * *".to_string()));
        assert!(metadata.auto_heartbeat);
        assert_eq!(metadata.heartbeat_interval_seconds, Some(30));
    }

    #[cfg(feature = "auto-register")]
    #[test]
    fn test_global_registry_available() {
        // Compiles only if GLOBAL_REGISTRY exists under the auto-register feature.
        use crate::worker::registry::GLOBAL_REGISTRY;
        let registry = GLOBAL_REGISTRY.read().unwrap();
        println!(
            "✅ Global registry available with {} tasks and {} workflows",
            registry.task_count(),
            registry.workflow_count()
        );
    }
}
