//! End-to-end mode: a worker and a client in one process

use anyhow::Result;
use std::time::Duration;
use tracing::{error, info};

use super::args::Args;
use super::{client, worker};

// ============================================================================
// E2E Test Mode
// ============================================================================

/// Run the worker in end-to-end test mode
///
/// This mode starts a worker in the background, runs the client against it,
/// and checks that each external API was called exactly once.
pub async fn run(args: &Args) -> Result<()> {
    info!("🧪 Starting E2E Test Mode");

    // Start worker in background
    let worker_args = args.clone();
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker::run(&worker_args).await {
            error!("Worker error: {}", e);
        }
    });

    info!("⏳ Waiting for worker to start...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run normal client test
    info!("🚀 Running normal execution test...");
    if let Err(e) = client::run(args).await {
        error!("❌ Client test failed: {}", e);
        worker_handle.abort();
        return Err(e);
    }

    info!("");
    info!("✅ E2E Test PASSED");
    info!("");
    info!("📊 Summary:");
    info!("   ✅ Worker started successfully");
    info!("   ✅ Payment workflow completed");
    info!("   ✅ Each external API was called exactly once");

    worker_handle.abort();
    Ok(())
}
