//! Workflow implementations for durable execution example
//!
//! This module contains all workflows that demonstrate ORCHER's durable execution:
//! - Payment workflow: Multi-step payment processing with automatic journaling

// Workflows auto-register with the worker via the `#[workflow]` macro; they are
// not imported by path.
pub mod payment;
