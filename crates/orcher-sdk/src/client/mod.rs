//! Client for starting and controlling workflows on the ORCHER orchestrator.
//!
//! A [`Client`] starts workflow executions and returns a [`WorkflowHandle`] for
//! each one. Through a handle you wait for the result, send events, run queries,
//! and cancel or terminate the workflow.
//!
//! ## Example
//!
//! ```rust,no_run
//! use orcher_sdk::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to the server
//!     let client = Client::connect("http://localhost:50051").await?;
//!
//!     // Start a workflow
//!     let handle = client
//!         .start_workflow("OrderWorkflow", "order-123")
//!         .await?;
//!
//!     // Wait for result
//!     let result: String = handle.result().await?;
//!     println!("Order result: {}", result);
//!
//!     // Send an event to a running workflow
//!     handle.send_event("approve", "approved").await?;
//!
//!     // Query workflow state
//!     let status: String = handle.query("getStatus", ()).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## ORCHER Terminology
//!
//! - **Event**: Asynchronous message sent to a workflow
//! - **Query**: Synchronous read of workflow state
//! - **Worker**: Process that executes workflows and tasks

mod client;
mod handle;
mod options;

pub use client::{Client, DataConverterType};
pub use handle::WorkflowHandle;
pub use options::{
    CancelOptions, ClientConfig, ClientTlsConfig, EventOptions, QueryOptions, QueryRejectCondition,
    StartWorkflowOptions, WorkflowIdReusePolicy,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_structure() {
        // Compiles only if every public type is reachable from this module.
        let _config: ClientConfig = ClientConfig::default();
        let _options: StartWorkflowOptions = StartWorkflowOptions::default();
        let _query_opts: QueryOptions = QueryOptions::default();
        let _event_opts: EventOptions = EventOptions::default();
    }
}
