//! The `#[query]` macro.
//!
//! A query inspects a running workflow's state without affecting execution. Query
//! functions must not be async and must have a return type. Attributes configure the name,
//! description, cache TTL, timeout, namespace and version.
//!
//! The macro is documented, with examples, on `#[query]` in the crate root.

pub mod r#macro;

pub use r#macro::query_impl;
