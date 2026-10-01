//! Derive macros, currently `#[derive(Payload)]` for payload serialization.

pub mod payload;

// Used by the `proc_macro_derive` entry point in `lib.rs`.
#[allow(unused_imports)]
pub use payload::derive_payload;
