//! gRPC client a worker uses to serve actor operations.
//!
//! [`ActorClient`] registers the worker's actor handlers, sends heartbeats,
//! long-polls for operations, reports their results, and reads and writes
//! actor state.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  Worker (SDK)                                               │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │  ActorClient                                        │    │
//! │  │                                                     │    │
//! │  │  - register_handlers()                              │    │
//! │  │  - heartbeat()                                      │    │
//! │  │  - poll_operations() / complete_operation_*()       │    │
//! │  │  - get_state() / set_state()                        │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                           │                                 │
//! │                           │ gRPC                            │
//! │                           ▼                                 │
//! └─────────────────────────────────────────────────────────────┘
//!                             │
//!                             │
//! ┌───────────────────────────▼─────────────────────────────────┐
//! │  Orcher Server                                              │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │  ActorService                                       │    │
//! │  │                                                     │    │
//! │  │  - RegisterHandlers                                 │    │
//! │  │  - Heartbeat                                        │    │
//! │  │  - PollActorOperation / CompleteActorOperation      │    │
//! │  │  - GetState / SetState                              │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Error, Result};
use orcher_proto::orcher::v1::{
    actor_service_client::ActorServiceClient, ActorHandler, GetStateRequest, GetStateResponse,
    HeartbeatRequest, HeartbeatResponse, OperationMode, RegisterHandlersRequest,
    RegisterHandlersResponse, SetStateRequest, SetStateResponse, WorkerMetrics, WorkerStatus,
};
use std::collections::HashMap;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

/// gRPC client a worker uses to serve actor operations.
#[derive(Debug, Clone)]
pub struct ActorClient {
    client: ActorServiceClient<Channel>,

    /// Identifies this worker to the server.
    service_id: String,

    /// Set by a successful registration; cleared when the server asks for re-registration.
    registration_id: Option<String>,
}

impl ActorClient {
    /// Connects to the server at `server_url` (such as `"http://localhost:50051"`).
    ///
    /// `service_id` identifies this worker in every request.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or the connection cannot be established.
    pub async fn new(server_url: impl Into<String>, service_id: impl Into<String>) -> Result<Self> {
        let server_url = server_url.into();
        let service_id = service_id.into();

        debug!(
            server_url = %server_url,
            service_id = %service_id,
            "Creating actor client"
        );

        let endpoint = Endpoint::from_shared(server_url.clone()).map_err(|e| {
            Error::Worker(crate::error::WorkerError::ConnectionFailed {
                url: server_url.clone(),
                reason: format!("Invalid server URL: {}", e),
            })
        })?;

        let channel = endpoint.connect().await.map_err(|e| {
            Error::Worker(crate::error::WorkerError::ConnectionFailed {
                url: server_url.clone(),
                reason: format!("Connection failed: {}", e),
            })
        })?;

        // Operations up to the configured message limit, not tonic's 4 MiB.
        let max = orcher_sdk_core::limits::default_max_message_bytes();
        let client = ActorServiceClient::new(channel)
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);

        info!(
            server_url = %server_url,
            service_id = %service_id,
            "Actor client connected"
        );

        Ok(Self {
            client,
            service_id,
            registration_id: None,
        })
    }

    /// Announces the actor handlers this worker can execute.
    ///
    /// Call it at startup, and again whenever a heartbeat asks for re-registration.
    /// `metadata` describes the worker (version, environment, and so on).
    ///
    /// A response with `success == false` is returned as `Ok`; check it. The
    /// registration ID is only stored on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn register_handlers(
        &mut self,
        handlers: Vec<ActorHandlerInfo>,
        metadata: HashMap<String, String>,
    ) -> Result<RegisterHandlersResponse> {
        info!(
            service_id = %self.service_id,
            handlers = handlers.len(),
            "Registering actor handlers"
        );

        let proto_handlers: Vec<ActorHandler> = handlers
            .into_iter()
            .map(|h| ActorHandler {
                actor_name: h.actor_name,
                operation: h.operation,
                mode: h.mode as i32,
                metadata: h.metadata,
            })
            .collect();

        let request = RegisterHandlersRequest {
            service_id: self.service_id.clone(),
            handlers: proto_handlers,
            metadata,
        };

        let response = self
            .client
            .register_handlers(request)
            .await
            .map_err(|e| {
                error!(
                    service_id = %self.service_id,
                    error = %e,
                    "Failed to register handlers"
                );
                Error::Worker(crate::error::WorkerError::StartupFailed(format!(
                    "Failed to register handlers: {}",
                    e
                )))
            })?
            .into_inner();

        if response.success {
            self.registration_id = Some(response.registration_id.clone());

            info!(
                service_id = %self.service_id,
                registration_id = %response.registration_id,
                handlers_registered = response.handlers_registered,
                "Successfully registered handlers"
            );
        } else {
            warn!(
                service_id = %self.service_id,
                error = %response.error_message,
                "Handler registration failed"
            );
        }

        Ok(response)
    }

    /// Reports that the worker is alive, with its status and optional metrics.
    ///
    /// Send heartbeats periodically. If the response has `re_register` set, the
    /// stored registration ID is cleared and the caller must call
    /// [`register_handlers`](Self::register_handlers) again.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn heartbeat(
        &mut self,
        status: WorkerStatus,
        metrics: Option<WorkerMetrics>,
    ) -> Result<HeartbeatResponse> {
        let registration_id = self.registration_id.clone().unwrap_or_default();

        debug!(
            service_id = %self.service_id,
            registration_id = %registration_id,
            status = ?status,
            "Sending heartbeat"
        );

        let request = HeartbeatRequest {
            service_id: self.service_id.clone(),
            registration_id,
            status: status as i32,
            metrics,
        };

        let response = self
            .client
            .heartbeat(request)
            .await
            .map_err(|e| {
                error!(
                    service_id = %self.service_id,
                    error = %e,
                    "Heartbeat failed"
                );
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Heartbeat failed: {}",
                    e
                )))
            })?
            .into_inner();

        if response.re_register {
            warn!(
                service_id = %self.service_id,
                "Server requested re-registration"
            );
            // is_registered() then returns false, which tells the caller to re-register.
            self.registration_id = None;
        }

        debug!(
            service_id = %self.service_id,
            success = response.success,
            re_register = response.re_register,
            "Heartbeat response received"
        );

        Ok(response)
    }

    /// Reads `state_key` of the instance `key` of `actor_name`.
    ///
    /// `execution_id` ties the read to the running operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn get_state(
        &mut self,
        actor_name: impl Into<String>,
        key: impl Into<String>,
        state_key: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Result<GetStateResponse> {
        let request = GetStateRequest {
            actor_name: actor_name.into(),
            key: key.into(),
            state_key: state_key.into(),
            execution_id: execution_id.into(),
        };

        let response = self
            .client
            .get_state(request)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to get state");
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Failed to get state: {}",
                    e
                )))
            })?
            .into_inner();

        Ok(response)
    }

    /// Writes the encoded `value` to `state_key` of the instance `key` of `actor_name`.
    ///
    /// With `expected_version`, the write only succeeds if the stored version matches
    /// (optimistic concurrency). A response with `success == false` is returned as `Ok`
    /// and logged; check it.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn set_state(
        &mut self,
        actor_name: impl Into<String>,
        key: impl Into<String>,
        state_key: impl Into<String>,
        value: Vec<u8>,
        execution_id: impl Into<String>,
        expected_version: Option<String>,
    ) -> Result<SetStateResponse> {
        let request = SetStateRequest {
            actor_name: actor_name.into(),
            key: key.into(),
            state_key: state_key.into(),
            value,
            execution_id: execution_id.into(),
            expected_version: expected_version.unwrap_or_default(),
        };

        let response = self
            .client
            .set_state(request)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to set state");
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Failed to set state: {}",
                    e
                )))
            })?
            .into_inner();

        if !response.success {
            warn!(
                error = %response.error_message,
                "State update failed"
            );
        }

        Ok(response)
    }

    /// Returns the service ID sent with every request.
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Returns the registration ID, if the worker is registered.
    pub fn registration_id(&self) -> Option<&str> {
        self.registration_id.as_deref()
    }

    /// Returns whether the worker holds a registration ID.
    pub fn is_registered(&self) -> bool {
        self.registration_id.is_some()
    }

    /// Long-polls for up to `max_operations` actor operations to execute.
    ///
    /// Returns when work is available or `timeout` elapses, possibly with an
    /// empty list. Call it in a loop to keep receiving work.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn poll_operations(
        &mut self,
        max_operations: u32,
        timeout: std::time::Duration,
    ) -> Result<Vec<orcher_proto::orcher::v1::ActorOperation>> {
        debug!(
            service_id = %self.service_id,
            max_operations = max_operations,
            timeout_ms = timeout.as_millis(),
            "Polling for actor operations"
        );

        let request = orcher_proto::orcher::v1::PollActorOperationRequest {
            service_id: self.service_id.clone(),
            max_operations,
            timeout_ms: timeout.as_millis() as u64,
        };

        let response = self
            .client
            .poll_actor_operation(request)
            .await
            .map_err(|e| {
                error!(
                    service_id = %self.service_id,
                    error = %e,
                    "Failed to poll for operations"
                );
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Failed to poll operations: {}",
                    e
                )))
            })?
            .into_inner();

        if !response.operations.is_empty() {
            debug!(
                service_id = %self.service_id,
                operations = response.operations.len(),
                "Received operations from poll"
            );
        }

        Ok(response.operations)
    }

    /// Reports that a polled operation succeeded with the encoded `result`.
    ///
    /// `operation_id` comes from the polled operation; `duration_ms` is how long it ran.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the server rejects the completion.
    pub async fn complete_operation_success(
        &mut self,
        operation_id: impl Into<String>,
        execution_id: impl Into<String>,
        result: Vec<u8>,
        duration_ms: u64,
    ) -> Result<()> {
        let operation_id = operation_id.into();
        let execution_id = execution_id.into();

        debug!(
            service_id = %self.service_id,
            operation_id = %operation_id,
            duration_ms = duration_ms,
            "Completing operation (success)"
        );

        let request = orcher_proto::orcher::v1::CompleteActorOperationRequest {
            service_id: self.service_id.clone(),
            operation_id: operation_id.clone(),
            execution_id,
            result,
            status: orcher_proto::orcher::v1::ExecutionStatus::Success as i32,
            error_message: String::new(),
            error_code: String::new(),
            duration_ms,
        };

        let response = self
            .client
            .complete_actor_operation(request)
            .await
            .map_err(|e| {
                error!(
                    service_id = %self.service_id,
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to complete operation"
                );
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Failed to complete operation: {}",
                    e
                )))
            })?
            .into_inner();

        if !response.success {
            warn!(
                service_id = %self.service_id,
                operation_id = %operation_id,
                error = %response.error_message,
                "Server rejected operation completion"
            );
            return Err(Error::Worker(crate::error::WorkerError::PollingError(
                format!("Failed to complete operation: {}", response.error_message),
            )));
        }

        Ok(())
    }

    /// Reports that a polled operation failed.
    ///
    /// `error_message` is for humans; `error_code` is for programs. Unlike
    /// [`complete_operation_success`](Self::complete_operation_success), a rejection by
    /// the server is only logged, not returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn complete_operation_error(
        &mut self,
        operation_id: impl Into<String>,
        execution_id: impl Into<String>,
        error_message: impl Into<String>,
        error_code: impl Into<String>,
        duration_ms: u64,
    ) -> Result<()> {
        let operation_id = operation_id.into();
        let execution_id = execution_id.into();
        let error_message = error_message.into();
        let error_code = error_code.into();

        warn!(
            service_id = %self.service_id,
            operation_id = %operation_id,
            error_message = %error_message,
            error_code = %error_code,
            "Completing operation (error)"
        );

        let request = orcher_proto::orcher::v1::CompleteActorOperationRequest {
            service_id: self.service_id.clone(),
            operation_id: operation_id.clone(),
            execution_id,
            result: Vec::new(),
            status: orcher_proto::orcher::v1::ExecutionStatus::Error as i32,
            error_message,
            error_code,
            duration_ms,
        };

        let response = self
            .client
            .complete_actor_operation(request)
            .await
            .map_err(|e| {
                error!(
                    service_id = %self.service_id,
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to complete operation"
                );
                Error::Worker(crate::error::WorkerError::PollingError(format!(
                    "Failed to complete operation: {}",
                    e
                )))
            })?
            .into_inner();

        if !response.success {
            warn!(
                service_id = %self.service_id,
                operation_id = %operation_id,
                error = %response.error_message,
                "Server rejected operation completion"
            );
        }

        Ok(())
    }
}

/// An actor operation this worker can execute, as sent at registration.
#[derive(Debug, Clone)]
pub struct ActorHandlerInfo {
    /// Actor type name, such as `"ShoppingCart"`.
    pub actor_name: String,

    /// Operation name, such as `"add_item"`.
    pub operation: String,

    /// Concurrency mode.
    pub mode: OperationMode,

    /// Free-form metadata about the handler.
    pub metadata: HashMap<String, String>,
}

impl ActorHandlerInfo {
    /// Describes `operation` on `actor_name`, with no metadata.
    pub fn new(
        actor_name: impl Into<String>,
        operation: impl Into<String>,
        mode: OperationMode,
    ) -> Self {
        Self {
            actor_name: actor_name.into(),
            operation: operation.into(),
            mode,
            metadata: HashMap::new(),
        }
    }

    /// Adds a metadata entry, replacing any existing value for `key`.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_handler_info_creation() {
        let info = ActorHandlerInfo::new("ShoppingCart", "add_item", OperationMode::Exclusive);

        assert_eq!(info.actor_name, "ShoppingCart");
        assert_eq!(info.operation, "add_item");
        assert_eq!(
            info.mode as i32,
            orcher_proto::orcher::v1::OperationMode::Exclusive as i32
        );
        assert!(info.metadata.is_empty());
    }

    #[test]
    fn test_actor_handler_info_with_metadata() {
        let info = ActorHandlerInfo::new("ShoppingCart", "add_item", OperationMode::Exclusive)
            .with_metadata("version", "1.0.0")
            .with_metadata("env", "production");

        assert_eq!(info.metadata.get("version"), Some(&"1.0.0".to_string()));
        assert_eq!(info.metadata.get("env"), Some(&"production".to_string()));
    }
}
