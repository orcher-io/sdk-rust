//! Client mode implementation - starts workflows and waits for results

use anyhow::Result;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

use super::args::Args;
use crate::types::{reset_counters, PaymentInput, PaymentOutput};

// ============================================================================
// Client Mode
// ============================================================================

/// Run the worker in client mode
///
/// This mode connects to the ORCHER server as a client, starts a payment
/// workflow, and waits for the result. It verifies that idempotency is
/// maintained (each API is called exactly once).
pub async fn run(args: &Args) -> Result<()> {
    info!("📱 Starting in CLIENT mode");

    // Increase timeout to 120 seconds for multi-step workflows
    let config = ClientConfig::new(args.server_url.clone())
        .with_namespace(args.namespace.clone())
        .with_timeout(Duration::from_secs(120));
    let client = Client::with_config(config).await?;
    info!("✅ Connected to ORCHER server");

    // Reset counters
    reset_counters();

    let input = PaymentInput::new(
        format!("ord_{}", &Uuid::new_v4().to_string()[..8]),
        99.99,
        "tok_visa_4242".to_string(),
        "customer@example.com".to_string(),
    );

    info!("🚀 Starting payment workflow for order: {}", input.order_id);

    // The workflow id is generated.
    let options =
        StartWorkflowOptions::new(args.task_queue.clone()).with_namespace(args.namespace.clone());

    let handle = client
        .start_workflow_with_options("payment_workflow", input, options)
        .await?;

    info!("✅ Workflow started: {}", handle.id());
    info!("⏳ Waiting for result...");

    let result: PaymentOutput = handle.result().await?;
    if !result.is_successful() {
        return Err(anyhow::anyhow!(
            "Workflow finished without completing the payment (status: {})",
            result.status
        ));
    }

    info!("🎉 Workflow completed successfully!");
    info!("   Order ID: {}", result.order_id);
    info!("   Charge ID: {}", result.charge_id);
    info!("   Status: {}", result.status);
    info!("");
    info!("📊 API Call Verification:");
    info!(
        "   Stripe calls: {} (should be 1)",
        result.api_calls_made.stripe_calls
    );
    info!(
        "   Email calls: {} (should be 1)",
        result.api_calls_made.email_calls
    );
    info!(
        "   Inventory calls: {} (should be 1)",
        result.api_calls_made.inventory_calls
    );

    // Verify idempotency (skip in benchmark mode where counters are per-process)
    if args.skip_idempotency_check {
        info!("⏭️  Skipping idempotency verification (--skip-idempotency-check)");
    } else if result.api_calls_made.verify_idempotency() {
        info!("✅ IDEMPOTENCY VERIFIED - No duplicate API calls!");
    } else {
        error!("❌ IDEMPOTENCY FAILED - Duplicate API calls detected!");
        error!("   Note: This can happen in multi-worker mode where counters are per-process.");
        error!("   Use --skip-idempotency-check for multi-worker benchmarks.");
        return Err(anyhow::anyhow!("Idempotency verification failed"));
    }

    Ok(())
}
