//! Client for calling operations on an actor instance.
//!
//! [`ActorInvocationClient`] encodes inputs and decodes outputs as JSON and
//! delegates the gRPC call to `CoreActorClient`. The typed clients generated
//! by `#[operations]` (for example `CounterClient`) wrap it.

use crate::error::Result;
use orcher_sdk_core::client::CoreActorClient;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

/// Untyped handle to one actor instance.
///
/// Bound to an actor type and key. Inputs and outputs are JSON-encoded with `serde_json`.
/// Generated typed clients and `Client::invoke_actor()` are both built on it.
#[derive(Clone)]
pub struct ActorInvocationClient {
    core: Arc<CoreActorClient>,
    actor_name: String,
    key: String,
}

impl ActorInvocationClient {
    /// Creates a handle to the instance `key` of `actor_name`.
    pub fn new(
        core: Arc<CoreActorClient>,
        actor_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            core,
            actor_name: actor_name.into(),
            key: key.into(),
        }
    }

    /// Invokes `operation` and waits for its result.
    ///
    /// # Errors
    ///
    /// Returns an error if `input` cannot be encoded, the RPC fails or times out,
    /// or the response cannot be decoded as `O`.
    pub async fn invoke<I: Serialize, O: DeserializeOwned>(
        &self,
        operation: &str,
        input: &I,
        timeout_ms: Option<u64>,
    ) -> Result<O> {
        let payload = serde_json::to_vec(input)
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        let result = self
            .core
            .invoke_operation(
                self.actor_name.clone(),
                self.key.clone(),
                operation.to_string(),
                payload,
                timeout_ms,
                None,
            )
            .await?;

        let output = serde_json::from_slice(&result)
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        Ok(output)
    }

    /// Invokes `operation` and discards its result.
    ///
    /// This still waits for the operation to complete on the server; only the
    /// output is dropped. `_timeout_ms` is ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if `input` cannot be encoded or the RPC fails.
    pub async fn send<I: Serialize>(
        &self,
        operation: &str,
        input: &I,
        _timeout_ms: Option<u64>,
    ) -> Result<()> {
        let payload = serde_json::to_vec(input)
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        self.core
            .invoke_operation_no_wait(
                self.actor_name.clone(),
                self.key.clone(),
                operation.to_string(),
                payload,
            )
            .await?;

        Ok(())
    }

    /// Returns the actor type name.
    pub fn actor_name(&self) -> &str {
        &self.actor_name
    }

    /// Returns the instance key.
    pub fn key(&self) -> &str {
        &self.key
    }
}
