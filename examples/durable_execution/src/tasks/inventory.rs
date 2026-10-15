//! Inventory reservation task - Database integration

use orcher_sdk::prelude::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

use crate::types::{
    stats::{increment_inventory_for_workflow, INVENTORY_API_CALLS},
    InventoryResult,
};

// ============================================================================
// Inventory Task
// ============================================================================

/// Reserve inventory in database
///
/// This simulates a database update for inventory reservation.
/// In production, this would be a database transaction that reserves
/// inventory items for the order.
///
/// ## Idempotency
/// This task increments the INVENTORY_API_CALLS counter to track how many times
/// the actual database is updated. With durable execution, this should be called
/// exactly once per workflow, even if the workflow crashes and replays.
#[task(name = "reserve_inventory")]
pub async fn reserve_inventory(ctx: TaskContext, order_id: String) -> Result<InventoryResult> {
    // Get workflow ID for per-workflow tracking
    let workflow_id = ctx.workflow_id();

    info!(
        "📦 Updating inventory database for order: {} (workflow: {})",
        order_id, workflow_id
    );

    // Track this actual database update (both global and per-workflow)
    let call_count = INVENTORY_API_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
    increment_inventory_for_workflow(workflow_id);

    info!(
        "   ⚠️  ACTUAL DATABASE UPDATE #{} (workflow: {})",
        call_count, workflow_id
    );

    // Simulate database operation
    tokio::time::sleep(Duration::from_millis(50)).await;

    // In production, this would be:
    // db.execute("UPDATE inventory SET reserved = true WHERE order_id = ?", order_id).await?;

    let reservation_id = format!("rsv_{}", &Uuid::new_v4().to_string()[..8]);

    info!("✅ Inventory reserved: {}", reservation_id);

    Ok(InventoryResult::new(true, reservation_id))
}
