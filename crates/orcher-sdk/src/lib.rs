//! # Orcher Rust SDK
//!
//! Build durable workflows and tasks for the Orcher workflow engine.
//!
//! Workflows are async functions whose progress the engine records, so they
//! survive crashes and restarts and resume where they left off. Tasks are the
//! steps that touch the outside world, and are retried under a policy when they
//! fail. A [`Worker`] polls the engine and runs both; a [`Client`] starts
//! workflows and reads their results.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use orcher_sdk::prelude::*;
//!
//! // A worker is I/O-bound, so a single-threaded runtime is enough.
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<()> {
//!     let worker = Worker::builder()
//!         .server_url("http://localhost:50051")
//!         .task_queue("my-queue")
//!         .build()
//!         .await?;
//!
//!     worker.run().await
//! }
//! ```
//!
//! ## Tokio runtime
//!
//! A worker spends its time waiting on gRPC long-polls, so the single-threaded
//! runtime (`#[tokio::main(flavor = "current_thread")]`) is recommended: its
//! event loop sleeps while it waits, whereas the multi-threaded work-stealing
//! scheduler keeps its threads busy even when the worker is idle.
//!
//! Run CPU-heavy work with `tokio::task::spawn_blocking()`, which uses a
//! separate thread pool and does not hold up the event loop:
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[task]
//! async fn checksum(_ctx: TaskContext, data: Vec<u8>) -> Result<u64> {
//!     let sum = tokio::task::spawn_blocking(move || {
//!         // CPU-intensive work here.
//!         data.iter().map(|&b| u64::from(b)).sum::<u64>()
//!     })
//!     .await
//!     .map_err(|e| Error::Other(e.to_string()))?;
//!     Ok(sum)
//! }
//! ```
//!
//! ## Modules
//!
//! - [`workflow`](mod@workflow): the workflow context and the durable operations it offers
//! - [`task`](mod@task): the task context, retry policies and task options
//! - [`actor`](mod@actor): keyed actors with durable state
//! - [`worker`]: building and running a worker
//! - [`client`]: starting, querying and sending events to workflows
//! - [`error`]: the SDK's error types
//! - [`prelude`]: the imports most programs need

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![allow(clippy::module_inception)]

// Re-exported so that macro-generated `#[::orcher_sdk::ctor::ctor]` resolves in user crates.
#[cfg(feature = "auto-register")]
#[doc(hidden)]
pub extern crate ctor;

/// Crates that macro-generated code names through `::orcher_sdk::__private`, so a crate
/// using the macros needs no dependency on them. Not public API.
#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "auto-register")]
    pub use inventory;
    pub use serde_json;
}

pub mod actor;
pub mod client;
pub mod debugging;
pub mod error;
pub mod interceptor;
pub mod payload;
pub mod task;
pub mod worker;
pub mod workflow;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use actor::{ActorContext, ActorKey, SharedActorContext};
pub use client::Client;
pub use error::{Error, ErrorCode, Result, Severity};
pub use payload::{Payload, PayloadCodec};
pub use task::{Task, TaskContext};
pub use worker::{HealthReport, HealthStatus, RuntimeStats, Worker, WorkerBuilder};
pub use workflow::{Workflow, WorkflowContext};

pub use orcher_sdk_core::{
    ListWorkflowsOptions, ListWorkflowsSortOrder, SearchWorkflowsOptions,
    WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowListPage, WorkflowStatus,
};

pub use orcher_sdk_core::proto::orcher::v1::NamespaceInfo;

#[cfg(feature = "auto-register")]
pub use orcher_sdk_macros::{actor, event, operations, query, task, tasks, update, workflow};

#[cfg(feature = "derive")]
pub use orcher_sdk_macros::Payload;

/// The imports most programs need.
///
/// ```rust
/// use orcher_sdk::prelude::*;
/// ```
///
/// # Result type
///
/// Workflows and tasks must return [`orcher_sdk::Result`](crate::Result), which the
/// prelude exports, not `anyhow::Result`. Converting through `anyhow::Error`
/// loses the error's type, and the worker relies on that type to tell a
/// workflow that is suspending from one that has failed. An explicit
/// `use anyhow::Result;` overrides the prelude's `Result`, so do not import both
/// in workflow and task modules.
pub mod prelude {
    pub use crate::error::{Error, ErrorCode, Result, Severity};

    // Workflow and task contexts
    pub use crate::task::{Task, TaskContext};
    pub use crate::workflow::{
        ChildWorkflowHandle, ChildWorkflowOptions, ClosureExecution, RestartFreshOptions, Workflow,
        WorkflowContext,
    };

    // Actor types
    pub use crate::actor::{ActorContext, ActorKey, SharedActorContext};

    // Client types
    pub use crate::client::{Client, WorkflowHandle};

    // Workflow query/list types
    pub use crate::{
        ListWorkflowsOptions, SearchWorkflowsOptions, WorkflowExecutionDescription,
        WorkflowExecutionInfo, WorkflowListPage, WorkflowStatus,
    };

    // Worker types
    pub use crate::worker::{HealthReport, HealthStatus, RuntimeStats, Worker, WorkerBuilder};

    // Payload utilities
    pub use crate::payload::{from_payload, to_payload, Payload, PayloadCodec, PayloadExt};

    #[cfg(feature = "auto-register")]
    pub use orcher_sdk_macros::{actor, event, operations, query, task, tasks, update, workflow};

    #[cfg(feature = "derive")]
    pub use orcher_sdk_macros::Payload;

    // Commonly needed items from dependencies.
    pub use async_trait::async_trait;
    pub use serde::{Deserialize, Serialize};
}

/// The version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The name this SDK reports itself under.
pub const SDK_NAME: &str = "orcher-rust-sdk";

/// Calls the crate's generated `__orcher_force_registration()` function.
///
/// Most programs do not need this macro. With the `auto-register` feature,
/// every `#[task]` and `#[workflow]` registers itself when the program starts,
/// and [`WorkerBuilder::new`] collects them.
///
/// The macro is for crates that generate explicit registration code instead:
/// a build script writes `$OUT_DIR/auto_register.rs` defining
/// `__orcher_force_registration()`, and the crate root includes it with
/// `include!(concat!(env!("OUT_DIR"), "/auto_register.rs"));`. Without that
/// function in scope, the macro fails to compile with a "not found" error.
///
/// # Example
///
/// ```rust,no_run
/// use orcher_sdk::prelude::*;
///
/// #[task]
/// async fn my_task(ctx: TaskContext, input: String) -> Result<String> {
///     Ok(format!("Processed: {}", input))
/// }
///
/// #[workflow]
/// async fn my_workflow(ctx: WorkflowContext, input: String) -> Result<String> {
///     Ok(input)
/// }
///
/// // Defines `__orcher_force_registration()`; written by the build script.
/// // include!(concat!(env!("OUT_DIR"), "/auto_register.rs"));
/// # fn __orcher_force_registration() {}
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Registers my_task and my_workflow.
///     orcher_sdk::initialize_handlers!();
///
///     let service = Worker::builder()
///         .server_url("http://localhost:50051")
///         .build()
///         .await?;
///
///     service.run().await
/// }
/// ```
#[cfg(feature = "auto-register")]
#[macro_export]
macro_rules! initialize_handlers {
    () => {
        // Defined by the calling crate's generated `auto_register.rs`.
        __orcher_force_registration();
    };
}

/// Fails to compile: `initialize_handlers!` requires the `auto-register` feature.
#[cfg(not(feature = "auto-register"))]
#[macro_export]
macro_rules! initialize_handlers {
    () => {
        compile_error!(
            "initialize_handlers!() requires the 'auto-register' feature. \
             Enable it in Cargo.toml: orcher-sdk = { version = \"...\", features = [\"auto-register\"] }"
        );
    };
}
