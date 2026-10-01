//! Static actor metadata collected for auto-registration.
//!
//! The `#[actor]` and `#[operations]` macros submit [`ActorDefinition`] and
//! `ActorHandlerRegistration` entries to `inventory`, so a worker can discover
//! every actor linked into the binary without listing them by hand.

use serde::{Deserialize, Serialize};

#[cfg(feature = "auto-register")]
use crate::actor::executor::ActorExecutor;

/// Metadata for a single actor operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetadata {
    /// Actor type name, such as `"ShoppingCart"`.
    pub actor_name: String,

    /// Operation name, such as `"add_item"`.
    pub operation_name: String,

    /// Concurrency mode.
    pub mode: OperationMode,
}

/// Concurrency mode of an operation, as declared in the actor definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationMode {
    /// Single writer; serialized per key.
    Exclusive,

    /// Intended for read-only operations.
    Shared,
}

/// Everything needed to register one actor and its operations with the server.
#[derive(Debug, Clone)]
pub struct ActorDefinition {
    /// Actor type name, such as `"ShoppingCart"`.
    pub actor_name: &'static str,

    /// Every operation defined on the actor.
    pub operations: &'static [OperationMetadata],
}

#[cfg(feature = "auto-register")]
inventory::collect!(ActorDefinition);

impl ActorDefinition {
    /// Creates a definition. `const` so the macros can build it in a static.
    pub const fn new(actor_name: &'static str, operations: &'static [OperationMetadata]) -> Self {
        Self {
            actor_name,
            operations,
        }
    }

    /// Returns the actor type name.
    pub fn actor_name(&self) -> &str {
        self.actor_name
    }

    /// Returns the actor's operations.
    pub fn operations(&self) -> &[OperationMetadata] {
        self.operations
    }

    /// Returns the number of operations.
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// Function that registers one handler with an [`ActorExecutor`] at worker startup.
#[cfg(feature = "auto-register")]
pub type ActorHandlerRegistrationFn = fn(&ActorExecutor);

/// Inventory entry that registers one handler with an [`ActorExecutor`].
///
/// The macros submit one entry per operation; the worker invokes them all at startup.
///
/// # Example
///
/// Roughly what the macros generate:
///
/// ```rust
/// use orcher::actor::{ActorContext, ActorExecutor, ActorHandlerRegistration};
/// use orcher::Error;
/// use std::sync::Arc;
///
/// fn register_counter_increment(executor: &ActorExecutor) {
///     executor.register_handler(
///         "Counter",
///         "increment",
///         Arc::new(|_ctx: ActorContext, payload: Vec<u8>| {
///             Box::pin(async move {
///                 // Decode the payload, run the operation, encode the result.
///                 let delta: i64 = serde_json::from_slice(&payload)
///                     .map_err(|e| Error::Serialization(e.to_string()))?;
///                 serde_json::to_vec(&delta).map_err(|e| Error::Serialization(e.to_string()))
///             })
///         }),
///     );
/// }
///
/// inventory::submit! {
///     ActorHandlerRegistration::new(register_counter_increment)
/// }
/// ```
#[cfg(feature = "auto-register")]
#[derive(Clone, Copy)]
pub struct ActorHandlerRegistration {
    /// Registration function to invoke.
    pub register_fn: ActorHandlerRegistrationFn,
}

#[cfg(feature = "auto-register")]
inventory::collect!(ActorHandlerRegistration);

#[cfg(feature = "auto-register")]
impl ActorHandlerRegistration {
    /// Creates a registration entry. `const` so it can be used in `inventory::submit!`.
    pub const fn new(register_fn: ActorHandlerRegistrationFn) -> Self {
        Self { register_fn }
    }

    /// Runs the registration function against `executor`.
    pub fn register(&self, executor: &ActorExecutor) {
        (self.register_fn)(executor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_definition_accessors() {
        // A struct literal with an empty static slice is the simplest 'static definition.
        let definition = ActorDefinition {
            actor_name: "TestActor",
            operations: &[],
        };
        assert_eq!(definition.actor_name(), "TestActor");
        assert_eq!(definition.operation_count(), 0);
    }

    #[test]
    fn test_operation_mode_serialization() {
        let mode = OperationMode::Exclusive;
        let json = serde_json::to_string(&mode).unwrap();
        let deserialized: OperationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, deserialized);
    }

    #[test]
    fn test_operation_metadata() {
        let metadata = OperationMetadata {
            actor_name: "ShoppingCart".to_string(),
            operation_name: "add_item".to_string(),
            mode: OperationMode::Exclusive,
        };

        assert_eq!(metadata.actor_name, "ShoppingCart");
        assert_eq!(metadata.operation_name, "add_item");
        assert_eq!(metadata.mode, OperationMode::Exclusive);
    }
}
