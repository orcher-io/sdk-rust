//! The `#[event]` macro.
//!
//! An event is sent to a running workflow to trigger an action or a state change.
//! Event handlers must be async and, unlike queries, may modify workflow state. Attributes
//! configure the name, description, timeout, priority (0-100, higher is more important),
//! maximum queue size, namespace and version.
//!
//! The macro is documented, with examples, on `#[event]` in the crate root.

pub mod r#macro;

pub use r#macro::event_impl;
