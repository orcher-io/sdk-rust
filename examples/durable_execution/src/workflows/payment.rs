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
    use orcher_sdk::testing::{MockRegistry, MockTaskBuilder, TestWorkflowExecutor};
    use orcher_sdk::workflow::WorkflowExecution;
    use std::sync::{Arc, RwLock};

    // The workflow runs in memory, with each task answered by a mock keyed by
    // the task's name. A task body needs a worker to run, so the tasks
    // themselves are exercised by the live `--e2e` mode instead.
    #[tokio::test]
    async fn completes_the_payment() -> orcher_sdk::Result<()> {
        let mocks = Arc::new(RwLock::new(MockRegistry::new()));
        MockTaskBuilder::new("charge_card".to_string(), mocks.clone()).returns(ChargeResult::new(
            "ch_test".to_string(),
            99.99,
            "succeeded".to_string(),
        ));
        MockTaskBuilder::new("reserve_inventory".to_string(), mocks.clone())
            .returns(InventoryResult::new(true, "rsv_test".to_string()));
        MockTaskBuilder::new("send_confirmation_email".to_string(), mocks.clone())
            .returns(NotificationResult::new(true, "notif_test".to_string()));

        let execution = WorkflowExecution::new(
            "payment-test".to_string(),
            "run-1".to_string(),
            "payment_workflow".to_string(),
            "examples".to_string(),
            "durable-execution-queue".to_string(),
        );
        let input = PaymentInput::new(
            "ord_123".to_string(),
            99.99,
            "tok_visa".to_string(),
            "test@example.com".to_string(),
        );
        let output: PaymentOutput = TestWorkflowExecutor::new(mocks)
            .execute_workflow(payment_workflow, execution, input)
            .await?;

        assert!(output.is_successful());
        assert_eq!(output.order_id, "ord_123");
        assert_eq!(output.charge_id, "ch_test");
        Ok(())
    }
}
