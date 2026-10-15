//! CLI argument parsing for durable execution service

use clap::Parser;

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug, Clone)]
#[command(name = "durable-execution-service")]
#[command(about = "ORCHER Durable Execution Example Service")]
pub struct Args {
    /// Run in client mode (starts payment workflow)
    #[arg(long)]
    pub client: bool,

    /// Run E2E test with replay verification
    #[arg(long)]
    pub e2e: bool,

    /// ORCHER server URL
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    pub server_url: String,

    /// Task queue name
    #[arg(
        long,
        env = "ORCHER_TASK_QUEUE",
        default_value = "durable-execution-queue"
    )]
    pub task_queue: String,

    /// Namespace
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "examples")]
    pub namespace: String,

    /// Skip idempotency verification (for multi-worker benchmarks where
    /// counters are per-process and cannot be reliably verified)
    #[arg(long, env = "ORCHER_SKIP_IDEMPOTENCY_CHECK", default_value = "false")]
    pub skip_idempotency_check: bool,

    /// Identity this worker reports to the engine (the SDK picks one if unset)
    #[arg(long, env = "ORCHER_WORKER_ID")]
    pub worker_id: Option<String>,

    /// Number of concurrent workflow pollers
    #[arg(long, env = "ORCHER_WORKFLOW_POLLER_COUNT", default_value = "4")]
    pub workflow_poller_count: usize,

    /// Number of concurrent task pollers
    #[arg(long, env = "ORCHER_TASK_POLLER_COUNT", default_value = "4")]
    pub task_poller_count: usize,
}

impl Args {
    /// Determine which mode to run in
    pub fn get_mode(&self) -> RunMode {
        if self.e2e {
            RunMode::E2e
        } else if self.client {
            RunMode::Client
        } else {
            RunMode::Worker
        }
    }
}

/// Run mode for the service
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Run as worker (polls for tasks)
    Worker,
    /// Run as client (starts payment workflow)
    Client,
    /// Run end-to-end test
    E2e,
}

impl std::fmt::Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Worker => write!(f, "Worker"),
            RunMode::Client => write!(f, "Client"),
            RunMode::E2e => write!(f, "E2E Test"),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_mode_display() {
        assert_eq!(RunMode::Worker.to_string(), "Worker");
        assert_eq!(RunMode::Client.to_string(), "Client");
        assert_eq!(RunMode::E2e.to_string(), "E2E Test");
    }
}
