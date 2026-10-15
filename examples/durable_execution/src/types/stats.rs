//! API call statistics and counters for tracking idempotency
//!
//! This module provides per-workflow API call tracking to correctly verify
//! idempotency in concurrent workflow scenarios. Each workflow has its own
//! set of counters, so running 5 concurrent workflows will show each workflow
//! made exactly 1 API call (not 5 total across all workflows).

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use tracing::debug;

// ============================================================================
// Per-Workflow Counters (for concurrent execution)
// ============================================================================

/// Thread-safe storage for per-workflow API call statistics
static WORKFLOW_API_CALLS: Lazy<RwLock<HashMap<String, WorkflowApiCalls>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// API call counts for a single workflow
#[derive(Debug, Clone, Default)]
struct WorkflowApiCalls {
    stripe_calls: usize,
    email_calls: usize,
    inventory_calls: usize,
}

// ============================================================================
// Process-wide Counters (numbered in the task logs)
// ============================================================================

/// Tracks actual Stripe API calls across every workflow in this process
pub static STRIPE_API_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Tracks actual email sends across every workflow in this process
pub static EMAIL_API_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Tracks actual inventory updates across every workflow in this process
pub static INVENTORY_API_CALLS: AtomicUsize = AtomicUsize::new(0);

// ============================================================================
// Stats Types
// ============================================================================

/// Statistics about API calls made during workflow execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiCallStats {
    pub stripe_calls: usize,
    pub email_calls: usize,
    pub inventory_calls: usize,
}

impl ApiCallStats {
    /// Collect API call statistics for a specific workflow
    pub fn collect_for_workflow(workflow_id: &str) -> Self {
        let map = WORKFLOW_API_CALLS.read().unwrap();
        if let Some(calls) = map.get(workflow_id) {
            Self {
                stripe_calls: calls.stripe_calls,
                email_calls: calls.email_calls,
                inventory_calls: calls.inventory_calls,
            }
        } else {
            Self::default()
        }
    }

    /// Verify that each API was called exactly once
    pub fn verify_idempotency(&self) -> bool {
        self.stripe_calls == 1 && self.email_calls == 1 && self.inventory_calls == 1
    }
}

// ============================================================================
// Per-Workflow Counter Functions
// ============================================================================

/// Increment Stripe API call counter for a workflow
pub fn increment_stripe_for_workflow(workflow_id: &str) {
    let mut map = WORKFLOW_API_CALLS.write().unwrap();
    let entry = map.entry(workflow_id.to_string()).or_default();
    entry.stripe_calls += 1;
    debug!(
        workflow_id = %workflow_id,
        stripe_calls = entry.stripe_calls,
        "Incremented Stripe API call counter for workflow"
    );
}

/// Increment Email API call counter for a workflow
pub fn increment_email_for_workflow(workflow_id: &str) {
    let mut map = WORKFLOW_API_CALLS.write().unwrap();
    let entry = map.entry(workflow_id.to_string()).or_default();
    entry.email_calls += 1;
    debug!(
        workflow_id = %workflow_id,
        email_calls = entry.email_calls,
        "Incremented Email API call counter for workflow"
    );
}

/// Increment Inventory API call counter for a workflow
pub fn increment_inventory_for_workflow(workflow_id: &str) {
    let mut map = WORKFLOW_API_CALLS.write().unwrap();
    let entry = map.entry(workflow_id.to_string()).or_default();
    entry.inventory_calls += 1;
    debug!(
        workflow_id = %workflow_id,
        inventory_calls = entry.inventory_calls,
        "Incremented Inventory API call counter for workflow"
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Reset all API call counters to zero (both global and per-workflow)
pub fn reset_counters() {
    // Reset global counters
    STRIPE_API_CALLS.store(0, Ordering::Relaxed);
    EMAIL_API_CALLS.store(0, Ordering::Relaxed);
    INVENTORY_API_CALLS.store(0, Ordering::Relaxed);

    // Reset per-workflow counters
    let mut map = WORKFLOW_API_CALLS.write().unwrap();
    map.clear();

    debug!("Reset all API call counters (global and per-workflow)");
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Tests run in parallel and share the process-wide counters, so each one
    // uses its own workflow id and never resets the counters.

    #[test]
    fn test_verify_idempotency() {
        let once = ApiCallStats {
            stripe_calls: 1,
            email_calls: 1,
            inventory_calls: 1,
        };
        assert!(once.verify_idempotency());

        let twice = ApiCallStats {
            stripe_calls: 2,
            ..once
        };
        assert!(!twice.verify_idempotency());
    }

    #[test]
    fn test_collect_for_workflow() {
        increment_stripe_for_workflow("test-collect");
        increment_stripe_for_workflow("test-collect");
        increment_email_for_workflow("test-collect");

        let stats = ApiCallStats::collect_for_workflow("test-collect");
        assert_eq!(stats.stripe_calls, 2);
        assert_eq!(stats.email_calls, 1);
        assert_eq!(stats.inventory_calls, 0);
    }

    #[test]
    fn test_collect_for_nonexistent_workflow() {
        let stats = ApiCallStats::collect_for_workflow("nonexistent");
        assert_eq!(stats.stripe_calls, 0);
        assert_eq!(stats.email_calls, 0);
        assert_eq!(stats.inventory_calls, 0);
    }
}
