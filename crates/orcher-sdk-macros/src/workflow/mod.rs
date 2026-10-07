//! The `#[workflow]` macro.
//!
//! A workflow is an async function that takes a `WorkflowContext` and is registered so that
//! workers pick it up automatically. Arguments set the name, description, version, task
//! queue, timeout, maximum concurrent steps, enabled flag, cron schedule,
//! tags and retry policy (`retry_policy(...)`). Arguments the parser does not recognize are
//! ignored without an error; see `common::attrs::WorkflowAttrs`.
//!
//! The macro is documented, with examples, on `#[workflow]` in the crate root.

pub mod r#macro;

pub use r#macro::workflow_impl;
