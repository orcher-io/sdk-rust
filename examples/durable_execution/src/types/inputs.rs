//! Input data structures for workflows

use serde::{Deserialize, Serialize};

// ============================================================================
// Input Types
// ============================================================================

/// Input for payment processing workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentInput {
    /// Unique order identifier
    pub order_id: String,
    /// Payment amount in USD
    pub amount: f64,
    /// Credit card token from payment gateway
    pub card_token: String,
    /// Customer email for confirmation
    pub customer_email: String,
}

/// Input for the `charge_card` task.
///
/// Tasks take a single named-struct input rather than positional tuples so the
/// fields are self-documenting and the call site is type-checked at compile time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeInput {
    /// Payment amount in USD
    pub amount: f64,
    /// Credit card token from the payment gateway
    pub card_token: String,
}

/// Input for the `send_confirmation_email` task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyInput {
    /// Customer email address
    pub email: String,
    /// Charge id to reference in the confirmation
    pub charge_id: String,
}

impl PaymentInput {
    /// Create a new payment input
    pub fn new(order_id: String, amount: f64, card_token: String, customer_email: String) -> Self {
        Self {
            order_id,
            amount,
            card_token,
            customer_email,
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
    fn test_payment_input_creation() {
        let input = PaymentInput::new(
            "ord_123".to_string(),
            99.99,
            "tok_visa".to_string(),
            "test@example.com".to_string(),
        );

        assert_eq!(input.order_id, "ord_123");
        assert_eq!(input.amount, 99.99);
        assert_eq!(input.card_token, "tok_visa");
        assert_eq!(input.customer_email, "test@example.com");
    }

    #[test]
    fn test_payment_input_serialization() {
        let input = PaymentInput::new(
            "ord_123".to_string(),
            99.99,
            "tok_visa".to_string(),
            "test@example.com".to_string(),
        );

        let json = serde_json::to_string(&input).unwrap();
        let deserialized: PaymentInput = serde_json::from_str(&json).unwrap();

        assert_eq!(input.order_id, deserialized.order_id);
        assert_eq!(input.amount, deserialized.amount);
    }
}
