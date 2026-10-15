//! # Durable Execution Service
//!
//! This service demonstrates ORCHER's core durability feature: automatic journaling and replay.
//!
//! ## Key Concepts
//!
//! **Automatic Journaling**: Every `ctx.execute()` call is automatically journaled to PostgreSQL.
//! When a workflow crashes and restarts, completed steps replay from the journal instead of
//! re-executing, ensuring:
//! - External API calls (Stripe, email, webhooks) happen exactly once
//! - No duplicate charges, emails, or side effects
//! - Workflows can safely retry without additional idempotency logic
//!
//! ## Architecture
//!
//! ```text
//! Payment Workflow Execution:
//!
//! First Execution:
//!   Step 1: charge_card ──────► Stripe API ──► Journal: {"step":"charge_card","result":"ch_123"}
//!   Step 2: reserve_inventory ► DB Update ───► Journal: {"step":"reserve","result":"ok"}
//!   💥 CRASH (before Step 3)
//!
//! Replay Execution:
//!   Step 1: charge_card ──────► ❌ SKIP (replay from journal: "ch_123")
//!   Step 2: reserve_inventory ► ❌ SKIP (replay from journal: "ok")
//!   Step 3: send_confirmation ► ✅ EXECUTE (new step, not in journal)
//! ```
//!
//! ## Module Structure
//!
//! - `types/`: All data structures (inputs, outputs, stats)
//! - `workflows/`: Workflow implementations
//! - `tasks/`: Task implementations
//! - `cli/`: CLI commands and modes
//!
//! ## Usage
//!
//! ```bash
//! # Run a worker (from the repository root)
//! cargo run -p orcher-example-durable-execution
//!
//! # Start one payment workflow and wait for its result
//! cargo run -p orcher-example-durable-execution -- --client
//!
//! # Run a worker and a client in one process
//! cargo run -p orcher-example-durable-execution -- --e2e
//! ```

use anyhow::Result;
use clap::Parser;
use tracing::{error, info, Level};

// Import modules
mod cli;
mod tasks;
mod types;
mod workflows;

// Re-export for convenience
use cli::{args::Args, client, e2e, worker};

// Note: Task and workflow registration now happens automatically via inventory!
// No build script or manual initialization needed - just use the #[task] and #[workflow] macros

// ============================================================================
// Main Entry Point
// ============================================================================

// The multi-threaded runtime, because the engine's benchmark scripts drive this
// worker under load. A worker that is mostly idle is better served by
// `#[tokio::main(flavor = "current_thread")]`, whose event loop sleeps while the
// gRPC long-polls wait (see the crate docs, "Tokio runtime").
#[tokio::main]
async fn main() -> Result<()> {
    // Handlers are automatically registered via inventory - no initialization needed!
    // The #[task] and #[workflow] macros generate inventory::submit! calls

    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .init();

    // Parse CLI arguments
    let args = Args::parse();

    // Print banner
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║      ORCHER Durable Execution Example Worker          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Handlers are collected from inventory automatically by WorkerBuilder

    // Determine run mode and execute
    let mode = args.get_mode();
    info!("🚀 Starting in {} mode", mode);

    let result = match mode {
        cli::args::RunMode::Worker => worker::run(&args).await,
        cli::args::RunMode::Client => client::run(&args).await,
        cli::args::RunMode::E2e => e2e::run(&args).await,
    };

    match result {
        Ok(_) => {
            info!("👋 Service shutdown complete");
            Ok(())
        }
        Err(e) => {
            error!("❌ Service error: {}", e);
            Err(e)
        }
    }
}
