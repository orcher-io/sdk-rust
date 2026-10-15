//! Output data structures for workflows and tasks

use serde::{Deserialize, Serialize};

use super::stats::ApiCallStats;

// ============================================================================
// Workflow Output Types
// ============================================================================

/// Output from payment processing workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentOutput {
    /// Order identifier
    pub order_id: String,
    /// Stripe charge identifier
    pub charge_id: String,
    /// Whether inventory was successfully reserved
    pub inventory_reserved: bool,
    /// Whether confirmation email was sent
    pub email_sent: bool,
    /// Final workflow status
    pub status: String,
    /// Statistics about API calls made (for idempotency verification)
    pub api_calls_made: ApiCallStats,
}

impl PaymentOutput {
    /// Create a new payment output
    pub fn new(
        order_id: String,
        charge_id: String,
        inventory_reserved: bool,
        email_sent: bool,
        status: String,
        api_calls_made: ApiCallStats,
    ) -> Self {
        Self {
            order_id,
            charge_id,
            inventory_reserved,
            email_sent,
            status,
            api_calls_made,
        }
    }

    /// Check if the payment was completed successfully
    pub fn is_successful(&self) -> bool {
        self.status == "completed" && self.inventory_reserved && self.email_sent
    }
}

// ============================================================================
// Task Output Types
// ============================================================================

/// Result from charging a credit card
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeResult {
    /// Stripe charge identifier
    pub charge_id: String,
    /// Amount charged in USD
    pub amount: f64,
    /// Charge status (succeeded, failed, etc.)
    pub status: String,
}

impl ChargeResult {
    /// Create a new charge result
    pub fn new(charge_id: String, amount: f64, status: String) -> Self {
        Self {
            charge_id,
            amount,
            status,
        }
    }
}

/// Result from reserving inventory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryResult {
    /// Whether reservation was successful
    pub reserved: bool,
    /// Unique reservation identifier
    pub reservation_id: String,
}

impl InventoryResult {
    /// Create a new inventory result
    pub fn new(reserved: bool, reservation_id: String) -> Self {
        Self {
            reserved,
            reservation_id,
        }
    }
}

/// Result from sending a notification email
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationResult {
    /// Whether email was sent successfully
    pub sent: bool,
    /// Notification identifier
    pub notification_id: String,
}

impl NotificationResult {
    /// Create a new notification result
    pub fn new(sent: bool, notification_id: String) -> Self {
        Self {
            sent,
            notification_id,
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
    fn test_payment_output_creation() {
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

        assert_eq!(output.order_id, "ord_123");
        assert_eq!(output.charge_id, "ch_abc");
        assert!(output.is_successful());
    }

    #[test]
    fn test_charge_result_creation() {
        let charge = ChargeResult::new("ch_123".to_string(), 99.99, "succeeded".to_string());

        assert_eq!(charge.charge_id, "ch_123");
        assert_eq!(charge.amount, 99.99);
        assert_eq!(charge.status, "succeeded");
    }

    #[test]
    fn test_inventory_result_creation() {
        let result = InventoryResult::new(true, "rsv_123".to_string());

        assert!(result.reserved);
        assert_eq!(result.reservation_id, "rsv_123");
    }

    #[test]
    fn test_notification_result_creation() {
        let result = NotificationResult::new(true, "notif_123".to_string());

        assert!(result.sent);
        assert_eq!(result.notification_id, "notif_123");
    }

    #[test]
    fn test_payment_output_serialization() {
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

        let json = serde_json::to_string(&output).unwrap();
        let deserialized: PaymentOutput = serde_json::from_str(&json).unwrap();

        assert_eq!(output.order_id, deserialized.order_id);
        assert_eq!(output.charge_id, deserialized.charge_id);
    }
}
