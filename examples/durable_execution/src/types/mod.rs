//! Type definitions for durable execution example
//!
//! This module contains all data structures used in the durable execution service:
//! - Input types for workflows
//! - Output types from workflows and tasks
//! - API call statistics and counters for idempotency verification

pub mod inputs;
pub mod outputs;
pub mod stats;

// Re-export commonly used types
pub use inputs::*;
pub use outputs::*;
pub use stats::*;
