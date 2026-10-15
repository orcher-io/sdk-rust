//! CLI command implementations for durable execution service
//!
//! This module contains all command-line interface implementations:
//! - Worker mode: Runs as a worker, polling for tasks
//! - Client mode: Starts workflows and waits for results
//! - E2E test mode: Runs full integration tests

pub mod args;
pub mod client;
pub mod e2e;
pub mod worker;
