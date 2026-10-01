//! Handler registry and dispatcher for actor operations.
//!
//! [`ActorExecutor`] maps `(actor_name, operation)` to a handler and runs it.
//! Handlers are registered by hand or collected from the macro-generated inventory.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  ActorExecutor                                          │
//! │                                                         │
//! │  ┌───────────────────────────────────────────────────┐ │
//! │  │  Handler Registry                                 │ │
//! │  │  (actor_name, operation) → HandlerFn              │ │
//! │  └───────────────────────────────────────────────────┘ │
//! │                                                         │
//! │  execute(actor, key, op, payload) ─────→ Result       │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::actor::executor::ActorExecutor;
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
//! let executor = ActorExecutor::new();
//!
//! // Registers the handlers #[operations] generated for Counter. A worker
//! // registers every actor in the binary on its own.
//! Counter::register_handlers(&executor);
//!
//! // Arguments travel as a JSON tuple, results as JSON.
//! let payload = serde_json::to_vec(&(5,)).unwrap();
//! let context = create_test_context("Counter", "user-123").await;
//! let result = executor
//!     .execute("Counter", "user-123", "increment", payload, context)
//!     .await?;
//! assert_eq!(serde_json::from_slice::<i64>(&result).unwrap(), 5);
//! # Ok(())
//! # }
//! ```

use crate::actor::ActorContext;
use crate::error::{Error, Result};
mod actor_metrics {
    pub const OPERATIONS_TOTAL: &str = "orcher_actor_operations_total";
    pub const OPERATIONS_FAILED_TOTAL: &str = "orcher_actor_operations_failed_total";
    pub const OPERATIONS_EXECUTING: &str = "orcher_actor_operations_executing";
    pub const OPERATION_DURATION_SECONDS: &str = "orcher_actor_operation_duration_seconds";
}
use actor_metrics as actors;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Type-erased async handler for one actor operation.
///
/// Takes the context and the JSON-encoded input, and returns the JSON-encoded output.
pub type ActorHandlerFn = Arc<
    dyn Fn(ActorContext, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

/// A handler together with the actor and operation it serves.
#[derive(Clone)]
pub struct HandlerInfo {
    /// Actor type name.
    pub actor_name: String,

    /// Operation name.
    pub operation: String,

    /// The handler.
    pub handler: ActorHandlerFn,
}

/// Registry of actor operation handlers.
///
/// Cloning is cheap and clones share the same registry.
#[derive(Clone)]
pub struct ActorExecutor {
    /// Keyed by `(actor_name, operation)`.
    handlers: Arc<std::sync::RwLock<HashMap<(String, String), ActorHandlerFn>>>,
}

impl ActorExecutor {
    /// Creates an executor with no handlers.
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Registers `handler` for `operation` on `actor_name`.
    ///
    /// A later registration for the same pair replaces the earlier one.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock is poisoned.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorExecutor;
    /// use std::sync::Arc;
    ///
    /// let executor = ActorExecutor::new();
    /// executor.register_handler(
    ///     "Counter",
    ///     "increment",
    ///     Arc::new(|_ctx, _payload| Box::pin(async move {
    ///         // Decode the payload, run the operation, encode the result.
    ///         Ok(vec![])
    ///     }))
    /// );
    /// assert!(executor.has_handler("Counter", "increment"));
    /// ```
    pub fn register_handler(
        &self,
        actor_name: impl Into<String>,
        operation: impl Into<String>,
        handler: ActorHandlerFn,
    ) {
        let actor_name = actor_name.into();
        let operation = operation.into();

        debug!(
            actor = %actor_name,
            operation = %operation,
            "Registering actor handler"
        );

        let mut handlers = self.handlers.write().unwrap();
        handlers.insert((actor_name.clone(), operation.clone()), handler);

        info!(
            actor = %actor_name,
            operation = %operation,
            "Actor handler registered"
        );
    }

    /// Registers every handler generated by the `#[actor]` and `#[operations]` macros.
    ///
    /// Actor definitions found in the inventory are only logged here; they are
    /// sent to the server separately. Each `ActorHandlerRegistration` entry is
    /// then invoked, and that is what adds handlers to this executor.
    #[cfg(feature = "auto-register")]
    pub fn register_from_inventory(&self) {
        use crate::actor::definition::{ActorDefinition, ActorHandlerRegistration};

        info!("Registering actor handlers from inventory");

        let mut definition_count = 0;
        for actor_def in inventory::iter::<ActorDefinition> {
            debug!(
                actor = %actor_def.actor_name,
                operations = actor_def.operations.len(),
                "Discovered actor definition from inventory"
            );
            definition_count += 1;
        }

        debug!(
            definitions = definition_count,
            "Actor definitions discovered"
        );

        let mut handler_count = 0;
        for registration in inventory::iter::<ActorHandlerRegistration> {
            debug!("Invoking actor handler registration function");
            registration.register(self);
            handler_count += 1;
        }

        info!(
            definitions = definition_count,
            handlers = handler_count,
            "Actor handlers registered from inventory"
        );
    }

    /// Runs the handler for `operation` on `actor_name` and returns its encoded result.
    ///
    /// `key` is used only for logging; the handler reaches the instance through `context`.
    /// Duration, in-flight count, and success or failure are recorded as metrics.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::HandlerNotFound`](crate::error::WorkerError::HandlerNotFound)
    /// if no handler is registered for the pair, or the handler's own error.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock is poisoned.
    pub async fn execute(
        &self,
        actor_name: impl AsRef<str>,
        key: impl AsRef<str>,
        operation: impl AsRef<str>,
        payload: Vec<u8>,
        context: ActorContext,
    ) -> Result<Vec<u8>> {
        let actor_name = actor_name.as_ref();
        let key = key.as_ref();
        let operation = operation.as_ref();

        debug!(
            actor = %actor_name,
            key = %key,
            operation = %operation,
            payload_size = payload.len(),
            "Executing actor operation"
        );

        let handler = {
            let handlers = self.handlers.read().unwrap();
            handlers
                .get(&(actor_name.to_string(), operation.to_string()))
                .cloned()
        };

        let handler = match handler {
            Some(h) => h,
            None => {
                warn!(
                    actor = %actor_name,
                    operation = %operation,
                    "Handler not found"
                );
                return Err(Error::Worker(crate::error::WorkerError::HandlerNotFound {
                    handler_type: "actor".to_string(),
                    name: format!("{}::{}", actor_name, operation),
                }));
            }
        };

        info!(
            actor = %actor_name,
            key = %key,
            operation = %operation,
            "Invoking actor handler"
        );

        let labels = [
            ("actor", actor_name.to_string()),
            ("operation", operation.to_string()),
        ];
        metrics::gauge!(actors::OPERATIONS_EXECUTING, &labels).increment(1.0);
        let start = std::time::Instant::now();

        let outcome = handler(context, payload).await;

        let elapsed = start.elapsed().as_secs_f64();
        metrics::gauge!(actors::OPERATIONS_EXECUTING, &labels).decrement(1.0);
        metrics::histogram!(actors::OPERATION_DURATION_SECONDS, &labels).record(elapsed);

        match outcome {
            Ok(result) => {
                info!(
                    actor = %actor_name,
                    key = %key,
                    operation = %operation,
                    result_size = result.len(),
                    "Actor handler executed successfully"
                );
                metrics::counter!(actors::OPERATIONS_TOTAL, &labels).increment(1);
                Ok(result)
            }
            Err(e) => {
                error!(
                    actor = %actor_name,
                    key = %key,
                    operation = %operation,
                    error = %e,
                    "Actor handler execution failed"
                );
                metrics::counter!(actors::OPERATIONS_FAILED_TOTAL, &labels).increment(1);
                Err(e)
            }
        }
    }

    /// Returns whether a handler is registered for `operation` on `actor_name`.
    pub fn has_handler(&self, actor_name: impl AsRef<str>, operation: impl AsRef<str>) -> bool {
        let handlers = self.handlers.read().unwrap();
        handlers.contains_key(&(
            actor_name.as_ref().to_string(),
            operation.as_ref().to_string(),
        ))
    }

    /// Returns the number of registered `(actor, operation)` handlers.
    pub fn handler_count(&self) -> usize {
        let handlers = self.handlers.read().unwrap();
        handlers.len()
    }

    /// Returns the distinct actor names with at least one handler, sorted.
    pub fn actor_names(&self) -> Vec<String> {
        let handlers = self.handlers.read().unwrap();
        let mut names: Vec<String> = handlers.keys().map(|(actor, _)| actor.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    /// Returns the operation names registered for `actor_name`, sorted.
    pub fn actor_operations(&self, actor_name: impl AsRef<str>) -> Vec<String> {
        let actor_name = actor_name.as_ref();
        let handlers = self.handlers.read().unwrap();
        let mut ops: Vec<String> = handlers
            .keys()
            .filter(|(actor, _)| actor == actor_name)
            .map(|(_, op)| op.clone())
            .collect();
        ops.sort();
        ops
    }
}

impl Default for ActorExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ActorExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorExecutor")
            .field("handler_count", &self.handler_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = ActorExecutor::new();
        assert_eq!(executor.handler_count(), 0);
    }

    #[test]
    fn test_handler_registration() {
        let executor = ActorExecutor::new();

        let handler: ActorHandlerFn =
            Arc::new(|_ctx, _payload| Box::pin(async move { Ok(vec![1, 2, 3]) }));

        executor.register_handler("Counter", "increment", handler);

        assert_eq!(executor.handler_count(), 1);
        assert!(executor.has_handler("Counter", "increment"));
        assert!(!executor.has_handler("Counter", "decrement"));
    }

    #[test]
    fn test_actor_names() {
        let executor = ActorExecutor::new();

        let handler: ActorHandlerFn =
            Arc::new(|_ctx, _payload| Box::pin(async move { Ok(vec![]) }));

        executor.register_handler("Counter", "increment", handler.clone());
        executor.register_handler("Counter", "decrement", handler.clone());
        executor.register_handler("Cart", "add_item", handler);

        let names = executor.actor_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"Counter".to_string()));
        assert!(names.contains(&"Cart".to_string()));
    }

    #[test]
    fn test_actor_operations() {
        let executor = ActorExecutor::new();

        let handler: ActorHandlerFn =
            Arc::new(|_ctx, _payload| Box::pin(async move { Ok(vec![]) }));

        executor.register_handler("Counter", "increment", handler.clone());
        executor.register_handler("Counter", "decrement", handler.clone());
        executor.register_handler("Cart", "add_item", handler);

        let ops = executor.actor_operations("Counter");
        assert_eq!(ops.len(), 2);
        assert!(ops.contains(&"increment".to_string()));
        assert!(ops.contains(&"decrement".to_string()));
    }
}
