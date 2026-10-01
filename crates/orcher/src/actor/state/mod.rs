//! Actor state access for handlers.
//!
//! [`ActorStateManager`] is the typed API handlers use through `ActorContext`.
//! It delegates to [`ActorStateClient`], which reads and writes state on the server over gRPC.
//! [`MockStateBackend`] replaces the server in tests.
//!
//! ## Layers
//!
//! ```text
//! Handler Code
//!     │
//!     ▼
//! ActorContext::state()
//!     │
//!     ▼
//! ActorStateManager (convenience API)
//!     │
//!     ▼
//! ActorStateClient (gRPC calls to server)
//!     │
//!     ▼
//! Server State Storage
//! ```
//!
//! ## Example
//!
//! ```rust
//! use orcher::prelude::*;
//!
//! #[derive(Serialize, Deserialize)]
//! pub struct Item {
//!     name: String,
//! }
//!
//! # #[actor]
//! # pub struct ShoppingCart;
//! # #[operations]
//! # impl ShoppingCart {
//! #[operation(exclusive)]
//! pub async fn add_item(ctx: ActorContext, item: Item) -> Result<()> {
//!     let state = ctx.state();
//!     let mut cart: Vec<Item> = state
//!         .get("cart")
//!         .await?
//!         .unwrap_or_default();
//!
//!     cart.push(item);
//!     state.set("cart", &cart).await?;
//!
//!     Ok(())
//! }
//! # }
//! ```

pub mod client;
pub mod manager;
pub mod mock;

pub use client::{ActorStateClient, ActorStateClientConfig};
pub use manager::ActorStateManager;
pub use mock::MockStateBackend;
