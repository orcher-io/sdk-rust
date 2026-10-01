//! Macros for stateful actors: `#[actor]` on the state struct and `#[operations]` on the
//! impl block that defines its operations.

pub mod actor_macro;
pub mod attrs;
pub mod codegen;
pub mod operations_macro;
pub mod registration;

pub use actor_macro::actor_impl;
pub use operations_macro::operations_impl;
