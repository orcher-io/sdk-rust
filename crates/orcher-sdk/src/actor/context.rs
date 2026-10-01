//! Execution context passed to actor operation handlers.
//!
//! Handlers run in the worker, but actor state stays authoritative on the server.
//! Every state read and write goes over RPC through [`ActorStateClient`].
//!
//! ## Architecture
//!
//! ```text
//! Worker Process                    Server Process
//! ┌─────────────────────────┐      ┌──────────────────────┐
//! │ #[actor]                │      │                      │
//! │ impl ShoppingCart {     │      │  Operation routing   │
//! │   fn add_item(ctx, ..)  │      │    ↓                 │
//! │     ↓                   │      │  State storage       │
//! │   ctx.state().get()     │ RPC  │                      │
//! │     ↓                   │ ---> │                      │
//! │   ActorContext          │      │                      │
//! │     ↓                   │      │                      │
//! │   ActorStateClient      │      │                      │
//! └─────────────────────────┘      └──────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! pub struct Item {
//!     name: String,
//! }
//!
//! #[actor]
//! pub struct ShoppingCart;
//!
//! #[operations]
//! impl ShoppingCart {
//!     #[operation(exclusive)]
//!     pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Vec<Item>> {
//!         // Each state call is an RPC to the server.
//!         let mut items = ctx
//!             .state()
//!             .get::<Vec<Item>>("cart")
//!             .await?
//!             .unwrap_or_default();
//!
//!         items.push(item);
//!
//!         ctx.state().set("cart", &items).await?;
//!
//!         Ok(items)
//!     }
//! }
//! ```

use crate::actor::state::{ActorStateClient, ActorStateManager};
use crate::actor::types::ActorKey;
use std::sync::Arc;
use uuid::Uuid;

/// Context for exclusive (read-write) operations.
///
/// Handlers marked `#[operation(exclusive)]` receive this context. It exposes the
/// actor's identity, the execution ID, and the instance's state, which is read and
/// written on the server over RPC.
///
/// # Examples
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// # #[actor]
/// # pub struct Account;
/// # #[operations]
/// # impl Account {
/// #[operation(exclusive)]
/// pub async fn deposit(ctx: ActorContext, amount: i64) -> Result<i64> {
///     let balance = ctx.state()
///         .get::<i64>("balance")
///         .await?
///         .unwrap_or(0);
///
///     let new_balance = balance + amount;
///     ctx.state().set("balance", &new_balance).await?;
///
///     Ok(new_balance)
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct ActorContext {
    actor_key: ActorKey,

    state_client: Arc<ActorStateClient>,

    /// Identifies this operation execution; used for tracing and idempotency.
    execution_id: String,

    /// Set when the operation was invoked from a workflow.
    workflow_execution_id: Option<String>,
}

impl ActorContext {
    /// Creates a context for one execution of an operation on `actor_key`.
    pub fn new(
        actor_key: ActorKey,
        state_client: Arc<ActorStateClient>,
        execution_id: String,
    ) -> Self {
        Self {
            actor_key,
            state_client,
            execution_id,
            workflow_execution_id: None,
        }
    }

    /// Returns the actor type name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # let ctx = create_test_context("ShoppingCart", "user-123").await;
    /// let actor_name = ctx.actor_name();
    /// assert_eq!(actor_name, "ShoppingCart");
    /// # }
    /// ```
    pub fn actor_name(&self) -> &str {
        self.actor_key.actor_name()
    }

    /// Returns the instance key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # let ctx = create_test_context("ShoppingCart", "user-123").await;
    /// let key = ctx.key();
    /// assert_eq!(key, "user-123");
    /// # }
    /// ```
    pub fn key(&self) -> &str {
        self.actor_key.key()
    }

    /// Returns the ID that uniquely identifies this operation execution.
    ///
    /// Use it for tracing, logging, and as an idempotency key for side effects.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Returns the full actor key (type name and instance key).
    pub fn actor_key(&self) -> &ActorKey {
        &self.actor_key
    }

    /// Returns the state manager for this actor instance.
    ///
    /// Every read and write it performs is an RPC to the server.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::ActorContext;
    /// # async fn example(ctx: &ActorContext) -> orcher_sdk::Result<()> {
    /// let value = ctx.state().get::<String>("name").await?;
    /// ctx.state().set("name", &"Alice".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn state(&self) -> ActorStateManager {
        ActorStateManager::new(
            self.actor_key.clone(),
            self.state_client.clone(),
            self.execution_id.clone(),
        )
    }

    /// Records the workflow execution that invoked this operation.
    ///
    /// Only used for observability; it does not change how state is accessed.
    pub fn with_workflow_execution_id(mut self, workflow_execution_id: String) -> Self {
        self.workflow_execution_id = Some(workflow_execution_id);
        self
    }

    /// Returns the invoking workflow execution ID, if there is one.
    pub fn workflow_execution_id(&self) -> Option<&str> {
        self.workflow_execution_id.as_deref()
    }

    /// Returns a UUID derived only from the actor key and `seed`.
    ///
    /// The same actor instance and seed always produce the same UUID, across
    /// executions and workers, so it is safe to use for stable identifiers.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let ctx = create_test_context("ShoppingCart", "user-123").await;
    /// let order_id = ctx.deterministic_uuid("order");
    ///
    /// // Another execution on the same instance derives the same id.
    /// let again = create_test_context("ShoppingCart", "user-123").await;
    /// assert_eq!(again.deterministic_uuid("order"), order_id);
    /// # }
    /// ```
    pub fn deterministic_uuid(&self, seed: &str) -> Uuid {
        // UUIDv5 namespaced per actor instance, so equal seeds on different
        // instances produce different IDs.
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_DNS,
            format!("{}:{}", self.actor_name(), self.key()).as_bytes(),
        );

        Uuid::new_v5(&namespace, seed.as_bytes())
    }
}

/// Context for shared (read-only) operations.
///
/// Handlers marked `#[operation(shared)]` receive this context. Shared operations
/// on the same key may run concurrently, so they should only read state.
///
/// # Examples
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// # #[actor]
/// # pub struct Account;
/// # #[operations]
/// # impl Account {
/// #[operation(shared)]
/// pub async fn get_balance(ctx: SharedActorContext) -> Result<i64> {
///     Ok(ctx.state()
///         .get::<i64>("balance")
///         .await?
///         .unwrap_or(0))
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct SharedActorContext {
    /// Same data as an [`ActorContext`]; read-only by convention.
    inner: ActorContext,
}

impl SharedActorContext {
    /// Creates a shared context for one execution of an operation on `actor_key`.
    pub fn new(
        actor_key: ActorKey,
        state_client: Arc<ActorStateClient>,
        execution_id: String,
    ) -> Self {
        Self {
            inner: ActorContext::new(actor_key, state_client, execution_id),
        }
    }

    /// Returns the actor type name.
    pub fn actor_name(&self) -> &str {
        self.inner.actor_name()
    }

    /// Returns the instance key.
    pub fn key(&self) -> &str {
        self.inner.key()
    }

    /// Returns the ID that uniquely identifies this operation execution.
    pub fn execution_id(&self) -> &str {
        self.inner.execution_id()
    }

    /// Returns the full actor key (type name and instance key).
    pub fn actor_key(&self) -> &ActorKey {
        self.inner.actor_key()
    }

    /// Returns the state manager for this actor instance.
    ///
    /// The returned manager does not prevent writes. Only exclusive operations
    /// are serialized per key; shared operations run concurrently, so writes
    /// from a shared handler can race. Use only `get()` and `list()` here.
    pub fn state(&self) -> ActorStateManager {
        self.inner.state()
    }

    /// Returns a UUID derived only from the actor key and `seed`.
    ///
    /// See [`ActorContext::deterministic_uuid`].
    pub fn deterministic_uuid(&self, seed: &str) -> Uuid {
        self.inner.deterministic_uuid(seed)
    }

    /// Returns the invoking workflow execution ID, if there is one.
    pub fn workflow_execution_id(&self) -> Option<&str> {
        self.inner.workflow_execution_id()
    }
}

impl From<ActorContext> for SharedActorContext {
    fn from(ctx: ActorContext) -> Self {
        Self { inner: ctx }
    }
}

/// Builder for [`ActorContext`] and [`SharedActorContext`].
///
/// The runtime uses it when executing operations; tests can use it too.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::actor::{ActorContextBuilder, ActorKey, ActorStateClient};
/// use std::sync::Arc;
///
/// let state_client = Arc::new(ActorStateClient::new_mock());
/// let ctx = ActorContextBuilder::new(
///     ActorKey::new("ShoppingCart", "user-123"),
///     state_client,
/// )
/// .execution_id("exec-456")
/// .build();
/// assert_eq!(ctx.execution_id(), "exec-456");
/// ```
pub struct ActorContextBuilder {
    actor_key: ActorKey,
    state_client: Arc<ActorStateClient>,
    execution_id: Option<String>,
    workflow_execution_id: Option<String>,
}

impl ActorContextBuilder {
    /// Starts a builder for `actor_key`.
    pub fn new(actor_key: ActorKey, state_client: Arc<ActorStateClient>) -> Self {
        Self {
            actor_key,
            state_client,
            execution_id: None,
            workflow_execution_id: None,
        }
    }

    /// Sets the execution ID. If unset, a random UUIDv4 is generated at build time.
    pub fn execution_id(mut self, execution_id: impl Into<String>) -> Self {
        self.execution_id = Some(execution_id.into());
        self
    }

    /// Sets the invoking workflow execution ID.
    pub fn workflow_execution_id(mut self, workflow_execution_id: impl Into<String>) -> Self {
        self.workflow_execution_id = Some(workflow_execution_id.into());
        self
    }

    /// Builds an exclusive [`ActorContext`].
    pub fn build(self) -> ActorContext {
        let execution_id = self
            .execution_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut ctx = ActorContext::new(self.actor_key, self.state_client, execution_id);

        if let Some(workflow_execution_id) = self.workflow_execution_id {
            ctx = ctx.with_workflow_execution_id(workflow_execution_id);
        }

        ctx
    }

    /// Builds a [`SharedActorContext`].
    pub fn build_shared(self) -> SharedActorContext {
        SharedActorContext::from(self.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::state::ActorStateClientConfig;

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_actor_context_creation() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );
        let execution_id = "exec-456".to_string();

        let ctx = ActorContext::new(actor_key.clone(), state_client, execution_id.clone());

        assert_eq!(ctx.actor_name(), "ShoppingCart");
        assert_eq!(ctx.key(), "user-123");
        assert_eq!(ctx.execution_id(), "exec-456");
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_shared_context_from_actor_context() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let ctx = ActorContext::new(actor_key, state_client, "exec-123".to_string());
        let shared_ctx = SharedActorContext::from(ctx);

        assert_eq!(shared_ctx.actor_name(), "ShoppingCart");
        assert_eq!(shared_ctx.key(), "user-123");
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_context_builder() {
        let actor_key = ActorKey::new("BankAccount", "account-789");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let ctx = ActorContextBuilder::new(actor_key, state_client)
            .execution_id("exec-999")
            .workflow_execution_id("workflow-111")
            .build();

        assert_eq!(ctx.actor_name(), "BankAccount");
        assert_eq!(ctx.key(), "account-789");
        assert_eq!(ctx.execution_id(), "exec-999");
        assert_eq!(ctx.workflow_execution_id(), Some("workflow-111"));
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_deterministic_uuid() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let ctx = ActorContext::new(actor_key, state_client, "exec-123".to_string());

        let uuid1 = ctx.deterministic_uuid("order");
        let uuid2 = ctx.deterministic_uuid("order");

        assert_eq!(uuid1, uuid2);

        let uuid3 = ctx.deterministic_uuid("invoice");
        assert_ne!(uuid1, uuid3);
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_context_builder_default_execution_id() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let ctx = ActorContextBuilder::new(actor_key, state_client).build();

        assert!(!ctx.execution_id().is_empty());
        assert!(Uuid::parse_str(ctx.execution_id()).is_ok());
    }
}
