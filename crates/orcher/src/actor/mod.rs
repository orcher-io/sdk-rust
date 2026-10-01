//! Worker-side actor runtime.
//!
//! Actors are durable, keyed virtual objects. Each key has at most one writer at a time,
//! and its state lives on the Orcher server rather than in the worker.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │  WORKER - USER CODE                                 │
//! │  ┌────────────────────────────────────────────────┐ │
//! │  │  #[actor]                                      │ │
//! │  │  pub struct ShoppingCart;                      │ │
//! │  │                                                │ │
//! │  │  #[operations]                                 │ │
//! │  │  impl ShoppingCart {                           │ │
//! │  │    #[operation(exclusive)]                     │ │
//! │  │    async fn add_item(ctx, item) { ... }        │ │
//! │  │  }                                             │ │
//! │  │                                                │ │
//! │  │  Handlers execute here                         │ │
//! │  └────────────────────────────────────────────────┘ │
//! │                                                     │
//! │  Worker::builder()                                  │
//! │    .build().await?                                  │
//! │    .run().await                                     │
//! └─────────────────────────────────────────────────────┘
//!                         │
//!                         │ RPC (state access + operation execution)
//!                         ▼
//! ┌─────────────────────────────────────────────────────┐
//! │  SERVER - COORDINATOR                               │
//! │  ┌────────────────────────────────────────────────┐ │
//! │  │  Routes operations to workers                  │ │
//! │  │  Enforces single writer per key                │ │
//! │  │  Persists state                                │ │
//! │  │  Does not execute handlers                     │ │
//! │  └────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key concepts
//!
//! - **Actor**: a stateful entity identified by a key. Each instance owns its own state.
//! - **Exclusive operation**: runs serialized per key and may read and write state.
//! - **Shared operation**: may run concurrently with other shared operations on the same
//!   key, so it should only read state.
//! - **State**: scoped by key as `actor:{actor_name}:{key}:{state_key}`, read and written
//!   over RPC, and persisted by the server. The worker may cache it locally.
//!
//! ## Example
//!
//! ```rust
//! use orcher::prelude::*;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! pub struct Item {
//!     name: String,
//!     price_cents: u64,
//!     quantity: u32,
//! }
//!
//! // Define an actor.
//! #[actor(name = "ShoppingCart")]
//! pub struct ShoppingCart;
//!
//! // Implement its operations.
//! #[operations]
//! impl ShoppingCart {
//!     #[operation(exclusive)]
//!     pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Vec<Item>> {
//!         let mut items = ctx
//!             .state()
//!             .get::<Vec<Item>>("cart")
//!             .await?
//!             .unwrap_or_default();
//!
//!         items.push(item);
//!         ctx.state().set("cart", &items).await?;
//!
//!         Ok(items)
//!     }
//!
//!     #[operation(shared)]
//!     pub async fn total_cents(ctx: SharedActorContext) -> Result<u64> {
//!         let items = ctx
//!             .state()
//!             .get::<Vec<Item>>("cart")
//!             .await?
//!             .unwrap_or_default();
//!
//!         Ok(items.iter().map(|i| i.price_cents * u64::from(i.quantity)).sum())
//!     }
//! }
//!
//! // A worker built in the same binary registers the actor automatically. Any
//! // process calls it through a client, by key:
//! async fn add_to_cart(client: &Client) -> Result<u64> {
//!     let cart = client.actor::<ShoppingCart>("user-123");
//!     let item = Item { name: "tea".to_string(), price_cents: 450, quantity: 2 };
//!     cart.add_item(item).await?;
//!     cart.total_cents().await
//! }
//! ```
//!
//! ## Modules
//!
//! - [`client`]: [`ActorClient`], the worker's lifecycle client (register, heartbeat, poll,
//!   complete).
//! - [`context`]: the handler contexts, [`ActorContext`] and [`SharedActorContext`].
//! - [`definition`]: static actor definitions collected for auto-registration.
//! - [`executor`]: [`ActorExecutor`], the handler registry and dispatcher.
//! - [`invocation`]: [`ActorInvocationClient`], for calling operations on other actors.
//! - [`state`]: state access; [`state::client`] talks gRPC, [`state::manager`] is the
//!   typed API handlers use.
//! - [`testing`]: helpers for unit-testing handlers without a server.
//! - [`types`]: core types such as [`ActorKey`] and [`OperationMode`].

pub mod client;
pub mod context;
pub mod definition;
pub mod executor;
pub mod invocation;
pub mod state;
pub mod testing;
pub mod types;

pub use client::{ActorClient, ActorHandlerInfo};
pub use context::{ActorContext, ActorContextBuilder, SharedActorContext};
pub use executor::{ActorExecutor, ActorHandlerFn};
pub use invocation::ActorInvocationClient;
pub use state::{ActorStateClient, ActorStateClientConfig, ActorStateManager, MockStateBackend};
pub use testing::{create_mock_state_client, create_test_context};
pub use types::{ActorKey, OperationMode, OperationResult};

// Implemented by the `#[actor]` and `#[operations]` macros.
pub use types::{Actor, ActorRef};

#[cfg(feature = "auto-register")]
pub use definition::{
    ActorDefinition, ActorHandlerRegistration, OperationMetadata, OperationMode as DefOperationMode,
};

/// The most commonly used actor types, for glob import.
///
/// ```rust
/// use orcher::actor::prelude::*;
/// ```
pub mod prelude {
    pub use super::client::{ActorClient, ActorHandlerInfo};
    pub use super::context::{ActorContext, SharedActorContext};
    pub use super::executor::ActorExecutor;
    pub use super::state::{ActorStateClient, ActorStateManager};
    pub use super::types::{Actor, ActorKey, OperationMode};

    #[cfg(feature = "auto-register")]
    pub use orcher_macros::{actor, operations};
}
