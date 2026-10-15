//! Payment processing workflow with automatic durability

use orcher_sdk::prelude::*;
use tracing::info;

use crate::tasks::{charge_card, reserve_inventory, send_confirmation_email};
use crate::types::{
    ApiCallStats, ChargeInput, ChargeResult, InventoryResult, NotificationResult, NotifyInput,
    PaymentInput, PaymentOutput,
};

// ============================================================================
// Payment Workflow
// ============================================================================

/// Payment processing workflow with automatic durability
///
/// This workflow demonstrates:
/// - Multi-step workflow with external API calls
/// - Automatic journaling of each step
/// - Replay protection on crash/restart
/// - No duplicate side effects
///
/// ## Steps:
/// 1. Charge credit card (Stripe API)
/// 2. Reserve inventory (Database update)
/// 3. Send confirmation email (Email API)
///
/// Each step is automatically journaled. If the workflow crashes and restarts,
/// completed steps replay from the journal without re-executing external APIs.
#[workflow(name = "payment_workflow")]
pub async fn payment_workflow(
    ctx: WorkflowContext,
    input: PaymentInput,
) -> orcher_sdk::Result<PaymentOutput> {
    info!("🚀 Starting payment workflow for order: {}", input.order_id);

    // Step 1: Charge credit card (external API - must be idempotent!)
    // `execute_task` takes a typed function reference — a typo or wrong input
    // type is a compile error, not a runtime failure.
    info!("💳 Step 1: Charging credit card...");
    let charge: ChargeResult = ctx
        .execute_task(
            charge_card,
            ChargeInput {
                amount: input.amount,
                card_token: input.card_token.clone(),
            },
        )
        .await?;
    info!("✅ Step 1 complete: Charge ID = {}", charge.charge_id);

    // Step 2: Reserve inventory (database update)
    info!("📦 Step 2: Reserving inventory...");
    let inventory: InventoryResult = ctx
        .execute_task(reserve_inventory, input.order_id.clone())
        .await?;
    info!(
        "✅ Step 2 complete: Reservation ID = {}",
        inventory.reservation_id
    );

    // Step 3: Send confirmation email (external API - must be idempotent!)
    info!("📧 Step 3: Sending confirmation email...");
    let notification: NotificationResult = ctx
        .execute_task(
            send_confirmation_email,
            NotifyInput {
                email: input.customer_email.clone(),
                charge_id: charge.charge_id.clone(),
            },
        )
        .await?;
    info!("✅ Step 3 complete: Email sent = {}", notification.sent);

    // Collect stats for this specific workflow (not global counters)
    // This ensures correct verification when running concurrent workflows
    let workflow_id = ctx.workflow_id();
    let api_calls = ApiCallStats::collect_for_workflow(workflow_id);

    let output = PaymentOutput::new(
        input.order_id,
        charge.charge_id,
        inventory.reserved,
        true,
        "completed".to_string(),
        api_calls.clone(),
    );

    info!("🎉 Payment workflow completed successfully");
    info!(
        "📊 API Call Stats (workflow {}): Stripe={}, Email={}, Inventory={}",
        ctx.workflow_id(),
        api_calls.stripe_calls,
        api_calls.email_calls,
        api_calls.inventory_calls
    );

    Ok(output)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // The idiomatic way to test a workflow is the SDK's `orcher_sdk::testing::TestEnv`
    // (mock_task + execute_workflow) — see docs/guides/sdk/rust/testing. The tasks
    // and end-to-end durability of this workflow are covered by the live `--e2e`
    // run against a real orchestrator (see `cli/e2e.rs`).

    #[test]
    fn test_payment_input_structure() {
        let input = PaymentInput::new(
            "ord_123".to_string(),
            99.99,
            "tok_visa".to_string(),
            "test@example.com".to_string(),
        );

        assert_eq!(input.order_id, "ord_123");
        assert_eq!(input.amount, 99.99);
    }

    #[test]
    fn test_payment_output_structure() {
        let stats = ApiCallStats {
            stripe_calls: 1,
            email_calls: 1,
            inventory_calls: 1,
        };

        let output = PaymentOutput::new(
            "ord_123".to_string(),
            "ch_abc".to_string(),
            true,
            true,
            "completed".to_string(),
            stats,
        );

        assert!(output.is_successful());
        assert_eq!(output.order_id, "ord_123");
    }
}
