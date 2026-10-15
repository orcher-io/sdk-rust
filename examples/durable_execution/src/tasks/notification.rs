//! Email notification task - Email service integration

use orcher_sdk::prelude::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

use crate::types::{
    stats::{increment_email_for_workflow, EMAIL_API_CALLS},
    NotificationResult, NotifyInput,
};

// ============================================================================
// Notification Task
// ============================================================================

/// Send confirmation email
///
/// This simulates an email API call (SendGrid, Mailgun, etc.)
/// In production, this would be a call to an email service provider
/// to send order confirmation emails to customers.
///
/// ## Idempotency
/// This task increments the EMAIL_API_CALLS counter to track how many times
/// the actual email API is called. With durable execution, this should be called
/// exactly once per workflow, even if the workflow crashes and replays.
#[task(name = "send_confirmation_email")]
pub async fn send_confirmation_email(
    ctx: TaskContext,
    input: NotifyInput,
) -> Result<NotificationResult> {
    let NotifyInput { email, charge_id } = input;

    // Get workflow ID for per-workflow tracking
    let workflow_id = ctx.workflow_id();

    info!(
        "📧 Sending confirmation email to: {} (workflow: {})",
        email, workflow_id
    );

    // Track this actual API call (both global and per-workflow)
    let call_count = EMAIL_API_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
    increment_email_for_workflow(workflow_id);

    info!(
        "   ⚠️  ACTUAL API CALL #{} to email service (workflow: {})",
        call_count, workflow_id
    );

    // Simulate email API call
    tokio::time::sleep(Duration::from_millis(80)).await;

    // In production, this would be:
    // sendgrid::send_email(&email, "Order Confirmation", &body).await?;
    // or
    // mailgun::send(&email, "Order Confirmation", &body).await?;

    let notification_id = format!("notif_{}", &Uuid::new_v4().to_string()[..8]);

    info!(
        "✅ Confirmation email sent (Charge ID: {}, Notification ID: {})",
        charge_id, notification_id
    );

    Ok(NotificationResult::new(true, notification_id))
}
