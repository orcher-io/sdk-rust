//! The `#[task]` and `#[tasks]` macros.
//!
//! A task is an async function that takes a `TaskContext` and runs as a unit of work inside
//! a workflow. Attributes configure the retry policy (exponential, linear or fixed), timeout,
//! resource requirements, conditional execution, parallelism and human approval. The macro
//! registers the task so that workers pick it up automatically.
//!
//! The macro is documented, with examples, on `#[task]` and `#[tasks]` in the crate root.

pub mod r#macro;
pub mod tasks_macro;

pub use r#macro::task_impl;
pub use tasks_macro::tasks_impl;
