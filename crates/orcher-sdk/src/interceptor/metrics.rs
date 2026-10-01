//! Built-in metrics interceptor.

use orcher_sdk_core::interceptor::{InterceptorContext, InterceptorHook, OperationType};
use std::sync::atomic::{AtomicU64, Ordering};

/// Collects execution counters for workflows and tasks.
///
/// The counters live in process memory and are read with
/// [`MetricsInterceptor::snapshot()`], which is enough for health checks and
/// debugging without setting up a metrics exporter.
///
/// ## Example
///
/// ```rust
/// # use orcher_sdk::interceptor::{InterceptorChain, MetricsInterceptor};
/// # use std::sync::Arc;
/// let metrics = Arc::new(MetricsInterceptor::new());
/// let mut chain = InterceptorChain::new();
/// chain.add_shared(metrics.clone());
///
/// // Later, read the counters.
/// let snap = metrics.snapshot();
/// println!("workflows started: {}", snap.workflow_started);
/// ```
pub struct MetricsInterceptor {
    workflow_started: AtomicU64,
    workflow_completed: AtomicU64,
    workflow_failed: AtomicU64,
    workflow_total_ms: AtomicU64,
    task_started: AtomicU64,
    task_completed: AtomicU64,
    task_failed: AtomicU64,
    task_total_ms: AtomicU64,
}

/// Snapshot of interceptor metrics at a point in time.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Number of workflow executions started.
    pub workflow_started: u64,
    /// Number of workflow executions that completed successfully.
    pub workflow_completed: u64,
    /// Number of workflow executions that failed.
    pub workflow_failed: u64,
    /// Total wall-clock milliseconds spent in completed workflows.
    pub workflow_total_ms: u64,
    /// Number of task executions started.
    pub task_started: u64,
    /// Number of task executions that completed successfully.
    pub task_completed: u64,
    /// Number of task executions that failed.
    pub task_failed: u64,
    /// Total wall-clock milliseconds spent in completed tasks.
    pub task_total_ms: u64,
}

impl MetricsInterceptor {
    /// Create a new metrics interceptor with all counters at zero.
    pub fn new() -> Self {
        Self {
            workflow_started: AtomicU64::new(0),
            workflow_completed: AtomicU64::new(0),
            workflow_failed: AtomicU64::new(0),
            workflow_total_ms: AtomicU64::new(0),
            task_started: AtomicU64::new(0),
            task_completed: AtomicU64::new(0),
            task_failed: AtomicU64::new(0),
            task_total_ms: AtomicU64::new(0),
        }
    }

    /// Take a consistent snapshot of all counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            workflow_started: self.workflow_started.load(Ordering::Relaxed),
            workflow_completed: self.workflow_completed.load(Ordering::Relaxed),
            workflow_failed: self.workflow_failed.load(Ordering::Relaxed),
            workflow_total_ms: self.workflow_total_ms.load(Ordering::Relaxed),
            task_started: self.task_started.load(Ordering::Relaxed),
            task_completed: self.task_completed.load(Ordering::Relaxed),
            task_failed: self.task_failed.load(Ordering::Relaxed),
            task_total_ms: self.task_total_ms.load(Ordering::Relaxed),
        }
    }
}

impl Default for MetricsInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl InterceptorHook for MetricsInterceptor {
    fn name(&self) -> &str {
        "metrics"
    }

    fn before_execution(&self, ctx: &InterceptorContext) {
        match ctx.operation_type {
            OperationType::Workflow => {
                self.workflow_started.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Task | OperationType::Actor => {
                self.task_started.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn after_execution(&self, ctx: &InterceptorContext, duration_ms: u64) {
        match ctx.operation_type {
            OperationType::Workflow => {
                self.workflow_completed.fetch_add(1, Ordering::Relaxed);
                self.workflow_total_ms
                    .fetch_add(duration_ms, Ordering::Relaxed);
            }
            OperationType::Task | OperationType::Actor => {
                self.task_completed.fetch_add(1, Ordering::Relaxed);
                self.task_total_ms.fetch_add(duration_ms, Ordering::Relaxed);
            }
        }
    }

    fn on_error(&self, ctx: &InterceptorContext, _error: &str, duration_ms: u64) {
        match ctx.operation_type {
            OperationType::Workflow => {
                self.workflow_failed.fetch_add(1, Ordering::Relaxed);
                self.workflow_total_ms
                    .fetch_add(duration_ms, Ordering::Relaxed);
            }
            OperationType::Task | OperationType::Actor => {
                self.task_failed.fetch_add(1, Ordering::Relaxed);
                self.task_total_ms.fetch_add(duration_ms, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let m = MetricsInterceptor::new();
        let wf_ctx = InterceptorContext::workflow("wf-1", "Order", 1, "default", "q");
        let task_ctx = InterceptorContext::task("t-1", "Process", 1, "default", "q");

        m.before_execution(&wf_ctx);
        m.after_execution(&wf_ctx, 50);

        m.before_execution(&task_ctx);
        m.on_error(&task_ctx, "boom", 10);

        let snap = m.snapshot();
        assert_eq!(snap.workflow_started, 1);
        assert_eq!(snap.workflow_completed, 1);
        assert_eq!(snap.workflow_failed, 0);
        assert_eq!(snap.workflow_total_ms, 50);
        assert_eq!(snap.task_started, 1);
        assert_eq!(snap.task_completed, 0);
        assert_eq!(snap.task_failed, 1);
        assert_eq!(snap.task_total_ms, 10);
    }
}
