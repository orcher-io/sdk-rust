//! Core actor types.
//!
//! - [`ActorKey`]: identifies an actor instance (actor name and key).
//! - [`OperationMode`]: whether an operation is exclusive or shared.
//! - [`OperationMetadata`]: an operation's name, actor, and mode.
//! - [`PendingOperation`]: an operation queued for execution.
//! - [`OperationResult`]: the encoded result of an operation.
//! - [`Actor`] and [`ActorRef`]: traits implemented by the actor macros.

use crate::actor::invocation::ActorInvocationClient;
use crate::error::WorkflowError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use tokio::sync::oneshot;
use uuid::Uuid;

// UUIDv5 namespace for `ActorKey::to_workflow_id`. These bytes are the standard DNS
// namespace (6ba7b810-9dad-11d1-80b4-00c04fd430c8). Changing them changes the workflow
// ID of every existing actor instance, so they must stay fixed.
const ACTOR_NAMESPACE_UUID: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Boxed future that runs one actor operation.
pub type OperationHandler = Pin<Box<dyn Future<Output = OperationResult> + Send>>;

/// Identifies an actor type by name.
///
/// Implemented by the `#[actor]` macro.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::actor::Actor;
/// use orcher_sdk::prelude::*;
///
/// #[actor]
/// pub struct ShoppingCart;
///
/// // The macro implements `Actor` with the struct's name.
/// assert_eq!(ShoppingCart::actor_name(), "ShoppingCart");
/// ```
pub trait Actor {
    /// Returns the actor type name.
    ///
    /// The name routes operations, labels metrics, and prefixes state keys
    /// (`actor:{name}:{key}`), so renaming an actor orphans its stored state.
    fn actor_name() -> &'static str;
}

/// Links an actor type to its generated typed client.
///
/// Implemented by the `#[operations]` macro. It lets `client.actor::<Counter>("key")`
/// return a `CounterClient` with one typed method per operation.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[actor]
/// pub struct Counter;
///
/// #[operations]
/// impl Counter {
///     #[operation(exclusive)]
///     pub async fn increment(ctx: ActorContext, delta: i64) -> Result<i64> {
///         let count = ctx.state().get::<i64>("count").await?.unwrap_or(0) + delta;
///         ctx.state().set("count", &count).await?;
///         Ok(count)
///     }
/// }
///
/// // `#[operations]` implements `ActorRef` for `Counter`, with `CounterClient` as its
/// // typed client:
/// async fn bump(client: &Client) -> Result<i64> {
///     let counter: CounterClient = client.actor::<Counter>("my-key");
///     counter.increment(10).await
/// }
/// ```
pub trait ActorRef: Actor {
    /// The generated typed client, such as `CounterClient`.
    type Client;

    /// Wraps an invocation handle in the typed client.
    fn client(handle: ActorInvocationClient) -> Self::Client;
}

/// Identifies one actor instance: an actor type name plus an instance key.
///
/// Operations are routed, and the single writer is enforced, per `ActorKey`.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::actor::ActorKey;
///
/// let key = ActorKey::new("ShoppingCart", "user-123");
/// assert_eq!(key.actor_name(), "ShoppingCart");
/// assert_eq!(key.key(), "user-123");
///
/// // Split back into its parts.
/// let (name, k) = key.as_tuple();
/// assert_eq!(name, "ShoppingCart");
/// assert_eq!(k, "user-123");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorKey {
    /// Actor type name, such as `"ShoppingCart"`.
    actor_name: String,
    /// Instance key, such as `"user-123"`.
    key: String,
}

impl ActorKey {
    /// Creates a key for instance `key` of `actor_name`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let key = ActorKey::new("BankAccount", "account-456");
    /// ```
    pub fn new(actor_name: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            actor_name: actor_name.into(),
            key: key.into(),
        }
    }

    /// Returns the actor type name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let key = ActorKey::new("ShoppingCart", "user-123");
    /// assert_eq!(key.actor_name(), "ShoppingCart");
    /// ```
    pub fn actor_name(&self) -> &str {
        &self.actor_name
    }

    /// Returns the instance key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let key = ActorKey::new("ShoppingCart", "user-123");
    /// assert_eq!(key.key(), "user-123");
    /// ```
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns `(actor_name, key)`, for matching and destructuring.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let actor_key = ActorKey::new("ShoppingCart", "user-123");
    /// let (name, key) = actor_key.as_tuple();
    /// assert_eq!(name, "ShoppingCart");
    /// assert_eq!(key, "user-123");
    /// ```
    pub fn as_tuple(&self) -> (&str, &str) {
        (&self.actor_name, &self.key)
    }

    /// Returns a workflow ID that is stable for this actor instance.
    ///
    /// It is a UUIDv5 of `"{actor_name}:{key}"`, so the same instance maps to the
    /// same ID across processes and restarts.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let key1 = ActorKey::new("OrderActor", "order-123");
    /// let key2 = ActorKey::new("OrderActor", "order-123");
    ///
    /// // Same actor instance, same workflow ID.
    /// assert_eq!(key1.to_workflow_id(), key2.to_workflow_id());
    ///
    /// let key3 = ActorKey::new("OrderActor", "order-456");
    /// // Different instance, different workflow ID.
    /// assert_ne!(key1.to_workflow_id(), key3.to_workflow_id());
    /// ```
    pub fn to_workflow_id(&self) -> Uuid {
        let identifier = format!("{}:{}", self.actor_name, self.key);
        Uuid::new_v5(&ACTOR_NAMESPACE_UUID, identifier.as_bytes())
    }

    /// Returns the prefix of every state key owned by this instance: `actor:{actor_name}:{key}`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorKey;
    ///
    /// let key = ActorKey::new("ShoppingCart", "user-123");
    /// assert_eq!(key.storage_prefix(), "actor:ShoppingCart:user-123");
    /// ```
    pub fn storage_prefix(&self) -> String {
        format!("actor:{}:{}", self.actor_name, self.key)
    }
}

impl fmt::Display for ActorKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.actor_name, self.key)
    }
}

/// Concurrency mode of an actor operation.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::actor::OperationMode;
///
/// let mode = OperationMode::Exclusive;
/// assert!(mode.is_exclusive());
/// assert!(!mode.is_shared());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationMode {
    /// At most one exclusive operation runs at a time per actor key.
    ///
    /// Use it for any operation that writes state.
    Exclusive,

    /// For read-only operations, which may run concurrently on the same key.
    ///
    /// Do not rely on concurrency: shared operations may be serialized like
    /// exclusive ones.
    Shared,
}

impl OperationMode {
    /// Returns whether the mode is [`OperationMode::Exclusive`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::OperationMode;
    ///
    /// assert!(OperationMode::Exclusive.is_exclusive());
    /// assert!(!OperationMode::Shared.is_exclusive());
    /// ```
    pub fn is_exclusive(&self) -> bool {
        matches!(self, OperationMode::Exclusive)
    }

    /// Returns whether the mode is [`OperationMode::Shared`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::OperationMode;
    ///
    /// assert!(OperationMode::Shared.is_shared());
    /// assert!(!OperationMode::Exclusive.is_shared());
    /// ```
    pub fn is_shared(&self) -> bool {
        matches!(self, OperationMode::Shared)
    }

    /// Returns `"exclusive"` or `"shared"`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::OperationMode;
    ///
    /// assert_eq!(OperationMode::Exclusive.as_str(), "exclusive");
    /// assert_eq!(OperationMode::Shared.as_str(), "shared");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationMode::Exclusive => "exclusive",
            OperationMode::Shared => "shared",
        }
    }
}

impl fmt::Display for OperationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Name, owning actor, and concurrency mode of an actor operation.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::actor::types::{OperationMetadata, OperationMode};
///
/// let metadata = OperationMetadata {
///     actor_name: "ShoppingCart".to_string(),
///     operation_name: "add_item".to_string(),
///     mode: OperationMode::Exclusive,
/// };
///
/// assert_eq!(metadata.actor_name, "ShoppingCart");
/// assert_eq!(metadata.operation_name, "add_item");
/// assert!(metadata.mode.is_exclusive());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetadata {
    /// Type name of the actor that owns the operation.
    pub actor_name: String,
    /// Operation name (the method name).
    pub operation_name: String,
    /// Concurrency mode.
    pub mode: OperationMode,
}

impl OperationMetadata {
    /// Creates metadata for `operation_name` on `actor_name`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::types::{OperationMetadata, OperationMode};
    ///
    /// let metadata = OperationMetadata::new(
    ///     "BankAccount",
    ///     "deposit",
    ///     OperationMode::Exclusive,
    /// );
    /// ```
    pub fn new(
        actor_name: impl Into<String>,
        operation_name: impl Into<String>,
        mode: OperationMode,
    ) -> Self {
        Self {
            actor_name: actor_name.into(),
            operation_name: operation_name.into(),
            mode,
        }
    }

    /// Returns the qualified name, `{actor_name}.{operation_name}`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::types::{OperationMetadata, OperationMode};
    ///
    /// let metadata = OperationMetadata::new(
    ///     "ShoppingCart",
    ///     "add_item",
    ///     OperationMode::Exclusive,
    /// );
    /// assert_eq!(metadata.qualified_name(), "ShoppingCart.add_item");
    /// ```
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.actor_name, self.operation_name)
    }
}

impl fmt::Display for OperationMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}::{} ({})",
            self.actor_name, self.operation_name, self.mode
        )
    }
}

/// Encoded result of an actor operation, or the error it failed with.
pub type OperationResult = Result<Vec<u8>, WorkflowError>;

/// An operation queued for an actor instance but not yet run.
///
/// Intended for actor dispatch internals; user code does not normally construct it.
pub struct PendingOperation {
    /// Random ID of this invocation.
    pub operation_id: Uuid,
    /// Operation being invoked.
    pub operation_name: String,
    /// Concurrency mode.
    pub mode: OperationMode,
    /// JSON-encoded input.
    pub payload: Vec<u8>,
    /// Receives the result for the caller.
    pub response_tx: oneshot::Sender<OperationResult>,
    /// When the operation was queued.
    pub enqueued_at: std::time::Instant,
    /// Future that runs the operation.
    pub handler: OperationHandler,
}

impl PendingOperation {
    /// Queues an operation, assigning a random ID and recording the enqueue time.
    pub fn new(
        operation_name: impl Into<String>,
        mode: OperationMode,
        payload: Vec<u8>,
        response_tx: oneshot::Sender<OperationResult>,
        handler: OperationHandler,
    ) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            operation_name: operation_name.into(),
            mode,
            payload,
            response_tx,
            enqueued_at: std::time::Instant::now(),
            handler,
        }
    }

    /// Returns how long the operation has been queued.
    pub fn wait_duration(&self) -> std::time::Duration {
        self.enqueued_at.elapsed()
    }
}

impl fmt::Debug for PendingOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingOperation")
            .field("operation_id", &self.operation_id)
            .field("operation_name", &self.operation_name)
            .field("mode", &self.mode)
            .field("payload_size", &self.payload.len())
            .field("enqueued_at", &self.enqueued_at)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_key_creation() {
        let key = ActorKey::new("ShoppingCart", "user-123");
        assert_eq!(key.actor_name(), "ShoppingCart");
        assert_eq!(key.key(), "user-123");
    }

    #[test]
    fn test_actor_key_display() {
        let key = ActorKey::new("BankAccount", "account-456");
        assert_eq!(key.to_string(), "BankAccount:account-456");
    }

    #[test]
    fn test_actor_key_storage_prefix() {
        let key = ActorKey::new("ShoppingCart", "user-123");
        assert_eq!(key.storage_prefix(), "actor:ShoppingCart:user-123");
    }

    #[test]
    fn test_actor_key_as_tuple() {
        let key = ActorKey::new("TestActor", "test-key");
        let (name, k) = key.as_tuple();
        assert_eq!(name, "TestActor");
        assert_eq!(k, "test-key");
    }

    #[test]
    fn test_actor_key_equality() {
        let key1 = ActorKey::new("Actor", "key1");
        let key2 = ActorKey::new("Actor", "key1");
        let key3 = ActorKey::new("Actor", "key2");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_actor_key_hashing() {
        use std::collections::HashMap;

        let key1 = ActorKey::new("Actor", "key1");
        let key2 = ActorKey::new("Actor", "key1");

        let mut map = HashMap::new();
        map.insert(key1.clone(), "value");

        assert_eq!(map.get(&key2), Some(&"value"));
    }

    #[test]
    fn test_operation_mode_is_exclusive() {
        assert!(OperationMode::Exclusive.is_exclusive());
        assert!(!OperationMode::Shared.is_exclusive());
    }

    #[test]
    fn test_operation_mode_is_shared() {
        assert!(OperationMode::Shared.is_shared());
        assert!(!OperationMode::Exclusive.is_shared());
    }

    #[test]
    fn test_operation_mode_as_str() {
        assert_eq!(OperationMode::Exclusive.as_str(), "exclusive");
        assert_eq!(OperationMode::Shared.as_str(), "shared");
    }

    #[test]
    fn test_operation_mode_display() {
        assert_eq!(OperationMode::Exclusive.to_string(), "exclusive");
        assert_eq!(OperationMode::Shared.to_string(), "shared");
    }

    #[test]
    fn test_operation_metadata_creation() {
        let metadata = OperationMetadata::new("Actor", "operation", OperationMode::Exclusive);
        assert_eq!(metadata.actor_name, "Actor");
        assert_eq!(metadata.operation_name, "operation");
        assert!(metadata.mode.is_exclusive());
    }

    #[test]
    fn test_operation_metadata_qualified_name() {
        let metadata = OperationMetadata::new("ShoppingCart", "add_item", OperationMode::Exclusive);
        assert_eq!(metadata.qualified_name(), "ShoppingCart.add_item");
    }

    #[test]
    fn test_operation_metadata_display() {
        let metadata = OperationMetadata::new("Actor", "op", OperationMode::Exclusive);
        assert_eq!(metadata.to_string(), "Actor::op (exclusive)");
    }

    #[test]
    fn test_pending_operation_creation() {
        let (tx, _rx) = oneshot::channel();
        let payload = vec![1, 2, 3, 4];
        let handler = Box::pin(async { Ok(vec![]) });
        let op = PendingOperation::new(
            "test_op",
            OperationMode::Exclusive,
            payload.clone(),
            tx,
            handler,
        );

        assert_eq!(op.operation_name, "test_op");
        assert_eq!(op.mode, OperationMode::Exclusive);
        assert_eq!(op.payload, payload);
    }

    #[test]
    fn test_pending_operation_wait_duration() {
        let (tx, _rx) = oneshot::channel();
        let handler = Box::pin(async { Ok(vec![]) });
        let op = PendingOperation::new("test_op", OperationMode::Exclusive, vec![], tx, handler);

        std::thread::sleep(std::time::Duration::from_millis(10));
        let duration = op.wait_duration();
        assert!(duration.as_millis() >= 10);
    }

    #[test]
    fn test_operation_mode_serialization() {
        let exclusive = OperationMode::Exclusive;
        let json = serde_json::to_string(&exclusive).unwrap();
        let deserialized: OperationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(exclusive, deserialized);
    }

    #[test]
    fn test_actor_key_serialization() {
        let key = ActorKey::new("TestActor", "test-123");
        let json = serde_json::to_string(&key).unwrap();
        let deserialized: ActorKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, deserialized);
    }
}
