//! Typed state API that actor handlers use through `ctx.state()`.
//!
//! [`ActorStateManager`] scopes every call to one actor instance, encodes values
//! as JSON, and reads and writes them on the server through [`ActorStateClient`].
//!
//! ## Architecture
//!
//! ```text
//! Worker Process                    Server Process
//! ┌─────────────────────────┐      ┌──────────────────────┐
//! │ Actor Operation         │      │                      │
//! │   ↓                     │      │  Operation routing   │
//! │ ctx.state().get("key")  │      │    ↓                 │
//! │   ↓                     │      │  State storage       │
//! │ ActorStateManager       │ RPC  │                      │
//! │   ↓                     │ ---> │                      │
//! │ ActorStateClient        │      │                      │
//! └─────────────────────────┘      └──────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::prelude::*;
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
//! pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Vec<Item>> {
//!     let state = ctx.state();
//!     let mut items = state
//!         .get::<Vec<Item>>("cart")
//!         .await?
//!         .unwrap_or_default();
//!
//!     items.push(item);
//!
//!     // The same manager sends the version it read, so a concurrent change fails the write.
//!     state.set("cart", &items).await?;
//!
//!     Ok(items)
//! }
//! # }
//! ```

use super::client::ActorStateClient;
use crate::actor::types::ActorKey;
use crate::error::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Typed access to the state of one actor instance.
///
/// Keys are scoped to the instance: state key `cart` on `ShoppingCart` instance
/// `user-123` is stored as `actor:ShoppingCart:user-123:cart`. Values are JSON, so
/// any `Serialize + DeserializeOwned` type can be stored.
///
/// # Optimistic concurrency
///
/// [`get`](Self::get) records the version it read for each key, and
/// [`set`](Self::set) sends that version so the server rejects the write if the
/// value changed in between. Versions live in this manager only. `ctx.state()`
/// returns a new manager on each call, so hold on to one manager across a
/// read-modify-write to get the check.
///
/// # Examples
///
/// ```rust
/// # use orcher_sdk::actor::testing::create_test_context;
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Item { id: u32, name: String }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> orcher_sdk::Result<()> {
/// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
/// // None if the key is not set.
/// let balance: Option<i64> = state_manager.get("balance").await?;
///
/// state_manager.set("balance", &1000i64).await?;
///
/// state_manager.delete("balance").await?;
///
/// let exists = state_manager.exists("balance").await?;
/// # Ok(())
/// # }
/// ```
pub struct ActorStateManager {
    actor_key: ActorKey,

    state_client: Arc<ActorStateClient>,

    /// Sent with every call to tie it to the running operation.
    execution_id: String,

    /// Last version read by get() for each state key; sent by set() as the expected version.
    versions: Mutex<HashMap<String, String>>,
}

impl ActorStateManager {
    /// Creates a manager for `actor_key` with no recorded versions.
    pub fn new(
        actor_key: ActorKey,
        state_client: Arc<ActorStateClient>,
        execution_id: String,
    ) -> Self {
        Self {
            actor_key,
            state_client,
            execution_id,
            versions: Mutex::new(HashMap::new()),
        }
    }

    /// Reads the value of state key `key`, or `None` if it is not set.
    ///
    /// Always reads from the server, bypassing the client cache, and records the
    /// version for the next [`set`](Self::set) of the same key.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the value cannot be decoded as `T`.
    ///
    /// # Panics
    ///
    /// Panics if the version map lock is poisoned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// let count: Option<u64> = state_manager.get("count").await?;
    ///
    /// let cart: Option<Vec<Item>> = state_manager.get("cart").await?;
    ///
    /// // Default when the key is not set.
    /// let balance = state_manager.get::<i64>("balance").await?.unwrap_or(0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let (value, version) = self
            .state_client
            .get_state_with_version(
                self.actor_key.actor_name(),
                self.actor_key.key(),
                key,
                &self.execution_id,
            )
            .await?;

        if !version.is_empty() {
            self.versions
                .lock()
                .unwrap()
                .insert(key.to_string(), version);
        }

        Ok(value)
    }

    /// Writes `value` to state key `key`.
    ///
    /// If this manager has read `key` before, the write carries the version it
    /// saw and fails if the value has changed since.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` cannot be encoded, the RPC fails, or the server
    /// rejects the write (including a version mismatch).
    ///
    /// # Panics
    ///
    /// Panics if the version map lock is poisoned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// state_manager.set("count", &42u64).await?;
    ///
    /// let cart = vec![Item { id: 1, name: "Widget".into() }];
    /// state_manager.set("cart", &cart).await?;
    ///
    /// // Read-modify-write on the same manager, so the version is checked.
    /// let mut balance = state_manager.get::<i64>("balance").await?.unwrap_or(0);
    /// balance += 100;
    /// state_manager.set("balance", &balance).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set<T>(&self, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let expected_version = self.versions.lock().unwrap().get(key).cloned();
        self.state_client
            .set_state(
                self.actor_key.actor_name(),
                self.actor_key.key(),
                key,
                value,
                &self.execution_id,
                expected_version.as_deref(),
            )
            .await
    }

    /// Deletes state key `key` and returns whether it existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// let existed = state_manager.delete("cart").await?;
    /// if existed {
    ///     println!("Cart was cleared");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.state_client
            .delete_state(
                self.actor_key.actor_name(),
                self.actor_key.key(),
                key,
                &self.execution_id,
            )
            .await
    }

    /// Returns whether state key `key` is set.
    ///
    /// This fetches the value (as [`get`](Self::get) does, recording its version)
    /// and discards it.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the stored value is not valid JSON.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// if state_manager.exists("cart").await? {
    ///     println!("Cart has items");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn exists(&self, key: &str) -> Result<bool> {
        // `serde_json::Value` accepts any stored JSON, whatever its Rust type.
        let result: Option<serde_json::Value> = self.get(key).await?;
        Ok(result.is_some())
    }

    /// Lists this instance's state keys, optionally only those starting with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// let all_keys = state_manager.list(None).await?;
    /// println!("Actor has {} state keys", all_keys.len());
    ///
    /// let cart_keys = state_manager.list(Some("cart_")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        self.state_client
            .list_state_keys(
                self.actor_key.actor_name(),
                self.actor_key.key(),
                &self.execution_id,
                prefix,
            )
            .await
    }

    /// Deletes every state key of this instance.
    ///
    /// Keys are listed, then deleted one RPC at a time. This is not atomic: if a
    /// delete fails, the keys deleted before it stay deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if listing or any delete fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// state_manager.clear_all().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_all(&self) -> Result<()> {
        let keys = self.list(None).await?;
        for key in keys {
            self.delete(&key).await?;
        }
        Ok(())
    }

    /// Reads several state keys, returning `(key, value)` pairs in the order given.
    ///
    /// Keys that are not set have `None`. Each key is a separate [`get`](Self::get)
    /// call, so this saves no round trips.
    ///
    /// # Errors
    ///
    /// Returns the first error from any read.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// let keys = vec!["cart", "wishlist", "favorites"];
    /// let results = state_manager.get_many::<Vec<Item>>(&keys).await?;
    ///
    /// for (key, value) in results {
    ///     if let Some(items) = value {
    ///         println!("{}: {} items", key, items.len());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_many<T>(&self, keys: &[&str]) -> Result<Vec<(String, Option<T>)>>
    where
        T: DeserializeOwned,
    {
        let mut results = Vec::with_capacity(keys.len());
        for &key in keys {
            let value = self.get(key).await?;
            results.push((key.to_string(), value));
        }
        Ok(results)
    }

    /// Reads `key`, passes the current value to `f`, writes the result, and returns it.
    ///
    /// The write carries the version from the read, so it fails rather than
    /// overwrite a concurrent change. It is not retried.
    ///
    /// # Errors
    ///
    /// Returns an error if the read or the write fails, including a version mismatch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use orcher_sdk::actor::testing::create_test_context;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Item { id: u32, name: String }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// # let state_manager = create_test_context("ShoppingCart", "user-123").await.state();
    /// let new_count = state_manager.update("count", |count: Option<u64>| {
    ///     count.unwrap_or(0) + 1
    /// }).await?;
    ///
    /// let cart = state_manager.update("cart", |cart: Option<Vec<Item>>| {
    ///     let mut items = cart.unwrap_or_default();
    ///     items.push(Item { id: 2, name: "Gadget".into() });
    ///     items
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update<T, F>(&self, key: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(Option<T>) -> T,
    {
        let current = self.get(key).await?;
        let new_value = f(current);
        self.set(key, &new_value).await?;
        Ok(new_value)
    }

    /// Returns the actor type name.
    pub fn actor_name(&self) -> &str {
        self.actor_key.actor_name()
    }

    /// Returns the instance key.
    pub fn key(&self) -> &str {
        self.actor_key.key()
    }

    /// Returns the execution ID sent with every call.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Returns the full actor key (type name and instance key).
    pub fn actor_key(&self) -> &ActorKey {
        &self.actor_key
    }
}

impl Clone for ActorStateManager {
    fn clone(&self) -> Self {
        Self {
            actor_key: self.actor_key.clone(),
            state_client: self.state_client.clone(),
            execution_id: self.execution_id.clone(),
            versions: Mutex::new(self.versions.lock().unwrap().clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::state::ActorStateClientConfig;

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_state_manager_creation() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );
        let execution_id = "exec-456".to_string();

        let manager = ActorStateManager::new(actor_key, state_client, execution_id);

        assert_eq!(manager.actor_name(), "ShoppingCart");
        assert_eq!(manager.key(), "user-123");
        assert_eq!(manager.execution_id(), "exec-456");
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_state_manager_accessors() {
        let actor_key = ActorKey::new("BankAccount", "account-789");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let manager =
            ActorStateManager::new(actor_key.clone(), state_client, "exec-999".to_string());

        assert_eq!(manager.actor_key(), &actor_key);
        assert_eq!(manager.actor_name(), "BankAccount");
        assert_eq!(manager.key(), "account-789");
    }

    #[tokio::test]
    #[ignore = "Integration test - requires running ORCHER server"]
    async fn test_state_manager_clone() {
        let actor_key = ActorKey::new("ShoppingCart", "user-123");
        let state_client = Arc::new(
            ActorStateClient::new(ActorStateClientConfig::default())
                .await
                .unwrap(),
        );

        let manager1 = ActorStateManager::new(actor_key, state_client, "exec-123".to_string());
        let manager2 = manager1.clone();

        assert_eq!(manager1.actor_name(), manager2.actor_name());
        assert_eq!(manager1.key(), manager2.key());
        assert_eq!(manager1.execution_id(), manager2.execution_id());
    }
}
