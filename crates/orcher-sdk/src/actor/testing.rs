//! Helpers for unit-testing actor handlers without a server.
//!
//! Every context created here uses in-memory state storage.
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::actor::testing::create_test_context;
//! use orcher_sdk::prelude::*;
//!
//! #[actor]
//! pub struct Counter;
//!
//! #[operations]
//! impl Counter {
//!     #[operation(exclusive)]
//!     pub async fn increment(ctx: ActorContext, delta: i64) -> Result<i64> {
//!         let count = ctx.state().get::<i64>("count").await?.unwrap_or(0) + delta;
//!         ctx.state().set("count", &count).await?;
//!         Ok(count)
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let ctx = create_test_context("Counter", "test-key").await;
//!
//! // `#[operations]` renames each operation to `<name>_impl`; call it directly.
//! assert_eq!(Counter::increment_impl(ctx.clone(), 5).await?, 5);
//! assert_eq!(Counter::increment_impl(ctx, 2).await?, 7);
//! # Ok(())
//! # }
//! ```

use crate::actor::state::ActorStateClient;
use crate::actor::{ActorContext, ActorKey};
use std::sync::Arc;

/// Creates a context for instance `key` of `actor_name`, backed by fresh in-memory state.
///
/// The execution ID is a random UUID. Each call gets its own storage; use
/// [`create_test_context_with_shared_state`] to share state between contexts.
///
/// # Example
///
/// ```rust
/// use orcher_sdk::actor::testing::create_test_context;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> orcher_sdk::Result<()> {
/// let ctx = create_test_context("Counter", "test-key").await;
/// ctx.state().set("count", &5).await?;
/// assert_eq!(ctx.state().get::<i64>("count").await?, Some(5));
/// # Ok(())
/// # }
/// ```
pub async fn create_test_context(actor_name: &str, key: &str) -> ActorContext {
    let actor_key = ActorKey::new(actor_name, key);
    let state_client = Arc::new(ActorStateClient::new_mock());
    let execution_id = uuid::Uuid::new_v4().to_string();

    ActorContext::new(actor_key, state_client, execution_id)
}

/// Like [`create_test_context`], but with a fixed `execution_id`.
///
/// # Example
///
/// ```rust
/// use orcher_sdk::actor::testing::create_test_context_with_execution_id;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let ctx = create_test_context_with_execution_id("Counter", "test-key", "exec-123").await;
/// assert_eq!(ctx.execution_id(), "exec-123");
/// # }
/// ```
pub async fn create_test_context_with_execution_id(
    actor_name: &str,
    key: &str,
    execution_id: &str,
) -> ActorContext {
    let actor_key = ActorKey::new(actor_name, key);
    let state_client = Arc::new(ActorStateClient::new_mock());

    ActorContext::new(actor_key, state_client, execution_id.to_string())
}

/// Creates a context that uses `state_client`, so several contexts can share storage.
///
/// State is still scoped per actor instance: contexts for different keys or actors
/// on the same client do not see each other's state. The execution ID is a random UUID.
///
/// # Example
///
/// ```rust
/// use orcher_sdk::actor::state::ActorStateClient;
/// use orcher_sdk::actor::testing::create_test_context_with_shared_state;
/// use std::sync::Arc;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> orcher_sdk::Result<()> {
/// let state_client = Arc::new(ActorStateClient::new_mock());
///
/// let first = create_test_context_with_shared_state("Counter", "key-1", state_client.clone()).await;
/// let again = create_test_context_with_shared_state("Counter", "key-1", state_client.clone()).await;
/// let other = create_test_context_with_shared_state("Counter", "key-2", state_client).await;
///
/// first.state().set("count", &1).await?;
///
/// // Same storage, but each context sees only its own instance's state.
/// assert_eq!(again.state().get::<i64>("count").await?, Some(1));
/// assert_eq!(other.state().get::<i64>("count").await?, None);
/// # Ok(())
/// # }
/// ```
pub async fn create_test_context_with_shared_state(
    actor_name: &str,
    key: &str,
    state_client: Arc<ActorStateClient>,
) -> ActorContext {
    let actor_key = ActorKey::new(actor_name, key);
    let execution_id = uuid::Uuid::new_v4().to_string();

    ActorContext::new(actor_key, state_client, execution_id)
}

/// Creates an in-memory state client to pass to
/// [`create_test_context_with_shared_state`].
///
/// # Example
///
/// ```rust
/// use orcher_sdk::actor::testing::{create_mock_state_client, create_test_context_with_shared_state};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let state_client = create_mock_state_client();
///
/// let ctx1 = create_test_context_with_shared_state("Actor1", "key1", state_client.clone()).await;
/// let ctx2 = create_test_context_with_shared_state("Actor2", "key2", state_client).await;
/// # }
/// ```
pub fn create_mock_state_client() -> Arc<ActorStateClient> {
    Arc::new(ActorStateClient::new_mock())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_test_context() {
        let ctx = create_test_context("TestActor", "test-key").await;

        assert_eq!(ctx.actor_name(), "TestActor");
        assert_eq!(ctx.key(), "test-key");
        assert!(!ctx.execution_id().is_empty());
    }

    #[tokio::test]
    async fn test_create_test_context_with_execution_id() {
        let ctx = create_test_context_with_execution_id("TestActor", "test-key", "exec-123").await;

        assert_eq!(ctx.execution_id(), "exec-123");
    }

    #[tokio::test]
    async fn test_create_test_context_with_shared_state() {
        let state_client = create_mock_state_client();

        let ctx1 =
            create_test_context_with_shared_state("TestActor", "key-1", state_client.clone()).await;

        let ctx2 =
            create_test_context_with_shared_state("TestActor", "key-2", state_client.clone()).await;

        ctx1.state().set("test", &42i64).await.unwrap();

        // Same actor, different instance key: the state is not visible.
        let value: Option<i64> = ctx2.state().get("test").await.unwrap();
        assert_eq!(value, None);

        // Same actor and instance key: the state is visible.
        let ctx3 =
            create_test_context_with_shared_state("TestActor", "key-1", state_client.clone()).await;

        let value: Option<i64> = ctx3.state().get("test").await.unwrap();
        assert_eq!(value, Some(42));
    }

    #[tokio::test]
    async fn test_state_persistence_across_contexts() {
        let state_client = create_mock_state_client();

        {
            let ctx =
                create_test_context_with_shared_state("Counter", "user-123", state_client.clone())
                    .await;

            ctx.state().set("count", &10i64).await.unwrap();
        }

        {
            let ctx =
                create_test_context_with_shared_state("Counter", "user-123", state_client.clone())
                    .await;

            let count: Option<i64> = ctx.state().get("count").await.unwrap();
            assert_eq!(count, Some(10));
        }
    }

    #[tokio::test]
    async fn test_state_isolation_between_keys() {
        let state_client = create_mock_state_client();

        let ctx1 =
            create_test_context_with_shared_state("Counter", "key-1", state_client.clone()).await;

        let ctx2 =
            create_test_context_with_shared_state("Counter", "key-2", state_client.clone()).await;

        ctx1.state().set("count", &10i64).await.unwrap();
        ctx2.state().set("count", &20i64).await.unwrap();

        let count1: Option<i64> = ctx1.state().get("count").await.unwrap();
        let count2: Option<i64> = ctx2.state().get("count").await.unwrap();

        assert_eq!(count1, Some(10));
        assert_eq!(count2, Some(20));
    }

    #[tokio::test]
    async fn test_state_isolation_between_actors() {
        let state_client = create_mock_state_client();

        let ctx1 =
            create_test_context_with_shared_state("Counter", "key-1", state_client.clone()).await;

        let ctx2 =
            create_test_context_with_shared_state("ShoppingCart", "key-1", state_client.clone())
                .await;

        // Same instance key and state key, different actors.
        ctx1.state().set("data", &10i64).await.unwrap();
        ctx2.state().set("data", &20i64).await.unwrap();

        let data1: Option<i64> = ctx1.state().get("data").await.unwrap();
        let data2: Option<i64> = ctx2.state().get("data").await.unwrap();

        assert_eq!(data1, Some(10));
        assert_eq!(data2, Some(20));
    }
}
