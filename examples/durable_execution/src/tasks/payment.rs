//! Payment charging task - Stripe API integration

use orcher_sdk::prelude::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

use crate::types::{
    stats::{increment_stripe_for_workflow, STRIPE_API_CALLS},
    ChargeInput, ChargeResult,
};

// ============================================================================
// Payment Task
// ============================================================================

/// Charge credit card via Stripe API
///
/// This simulates an external payment API call.
/// In production, this would be: stripe::Charge::create(...)
///
/// ## Idempotency
/// This task increments the STRIPE_API_CALLS counter to track how many times
/// the actual API is called. With durable execution, this should be called
/// exactly once per workflow, even if the workflow crashes and replays.
#[task(name = "charge_card", retry = 3)]
pub async fn charge_card(ctx: TaskContext, input: ChargeInput) -> Result<ChargeResult> {
    // A real charge would pass `input.card_token` to the gateway; the simulation
    // only needs the amount.
    let ChargeInput { amount, .. } = input;

    // Get workflow ID for per-workflow tracking
    let workflow_id = ctx.workflow_id();

    // Chaos (test-only, env-gated): fail the first execution per workflow to exercise
    // the durable retry path; the retry then succeeds, so the workflow still completes.
    // Under sharding this verifies the rescheduled retry co-locates on the workflow's
    // shard. A per-workflow marker file (shared local FS, visible to every worker
    // process on the host) makes the failure fire exactly once — `ctx.attempt()` is
    // always 0 for a durable step, so it can't gate this.
    if std::env::var("ORCHER_CHAOS_FAIL_FIRST").is_ok() {
        let marker = std::env::temp_dir().join(format!("orcher-chaos-{}", workflow_id));
        if !marker.exists() {
            let _ = std::fs::write(&marker, b"1");
            tracing::warn!(
                "🧪 chaos: failing charge_card first execution (workflow: {})",
                workflow_id
            );
            return Err(orcher_sdk::error::TaskError::execution_failed(
                "chaos: forced first-attempt failure",
            )
            .into());
        }
    }

    // Chaos (test-only): fail for the first N seconds after this workflow is first seen,
    // then succeed. Replay-safe (wall-clock, not an invocation counter, which inflates on
    // replay): the initial run + its early workflow-level retries all fail within the
    // window → the workflow fails and is retried → a later successor run (after the
    // window) succeeds and recovers. Global first-seen file → one workflow per host.
    if let Ok(secs) = std::env::var("ORCHER_CHAOS_FAIL_SECS") {
        let secs: u64 = secs.parse().unwrap_or(0);
        let marker = std::env::temp_dir().join("orcher-chaos-firstseen");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let start = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or_else(|| {
                let _ = std::fs::write(&marker, now.to_string());
                now
            });
        if now < start + secs {
            tracing::warn!(
                "🧪 chaos: failing charge_card ({}s into the {}s window, workflow: {})",
                now.saturating_sub(start),
                secs,
                workflow_id
            );
            return Err(orcher_sdk::error::TaskError::execution_failed(
                "chaos: forced failure within the retry window",
            )
            .into());
        }
        tracing::warn!(
            "🧪 chaos: charge_card past the {}s window — succeeding (workflow: {})",
            secs,
            workflow_id
        );
    }

    info!(
        "💳 Calling Stripe API to charge ${:.2} (workflow: {})",
        amount, workflow_id
    );

    // Track this actual API call (both global and per-workflow)
    let call_count = STRIPE_API_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
    let stripe_calls = increment_stripe_for_workflow(workflow_id);

    info!(
        "   ⚠️  ACTUAL API CALL #{} to Stripe (workflow: {})",
        call_count, workflow_id
    );

    // Simulate API call latency
    tokio::time::sleep(Duration::from_millis(100)).await;

    // In production, this would be:
    // let charge = stripe::Charge::create(&client, &params).await?;

    let charge_id = format!("ch_{}", &Uuid::new_v4().to_string()[..8]);

    info!("✅ Stripe charge successful: {}", charge_id);

    // The count travels in the task's result: the workflow may only use what
    // its tasks return, never state in this process.
    Ok(ChargeResult::new(
        charge_id,
        amount,
        "succeeded".to_string(),
        stripe_calls,
    ))
}
