//! The `#[update]` macro.
//!
//! An update handler reads and modifies workflow state through `WorkflowContext` and returns
//! a result to the caller. It must be async, must have a return type, and must take
//! `WorkflowContext` as its first parameter. It may call `ctx.execute_task()` for side effects.
//! The accepted update and its result are journaled (`UpdateAccepted` and `UpdateCompleted`
//! events) so that replay reproduces them.
//!
//! The macro is documented, with examples, on `#[update]` in the crate root.

pub mod r#macro;

pub use r#macro::update_impl;
