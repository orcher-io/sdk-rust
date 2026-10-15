//! Task implementations for durable execution example
//!
//! This module contains all tasks that interact with external systems:
//! - Payment task: Charges credit cards via Stripe API
//! - Inventory task: Reserves inventory in the database
//! - Notification task: Sends confirmation emails via email service
//!
//! Each task demonstrates idempotency tracking to verify that ORCHER's
//! durable execution prevents duplicate API calls during replay.

pub mod inventory;
pub mod notification;
pub mod payment;

// Re-export tasks for easier access
pub use inventory::reserve_inventory;
pub use notification::send_confirmation_email;
pub use payment::charge_card;
