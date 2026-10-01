//! Built-in tracing interceptor using `tracing` spans.

use orcher_sdk_core::interceptor::{InterceptorContext, InterceptorHook};

/// Wraps each workflow and task execution in a `tracing` span.
///
/// Each span carries the operation type, ID, name, attempt and task queue as
/// fields, and works with any `tracing` subscriber (console, Jaeger, OTLP, and
/// so on).
///
/// ## Example
///
/// ```rust
/// # use orcher::interceptor::{InterceptorChain, TracingInterceptor};
/// let mut chain = InterceptorChain::new();
/// chain.add(TracingInterceptor::new());
/// ```
pub struct TracingInterceptor {
    _private: (),
}

impl TracingInterceptor {
    /// Create a new tracing interceptor.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for TracingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl InterceptorHook for TracingInterceptor {
    fn name(&self) -> &str {
        "tracing"
    }

    fn before_execution(&self, ctx: &InterceptorContext) {
        tracing::debug_span!(
            "orcher.execution",
            otel.name = %ctx.operation_name,
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            name = %ctx.operation_name,
            attempt = ctx.attempt,
            task_queue = %ctx.task_queue,
            namespace = %ctx.namespace,
        )
        .in_scope(|| {
            tracing::debug!("Execution span created");
        });
    }

    fn after_execution(&self, ctx: &InterceptorContext, duration_ms: u64) {
        tracing::debug!(
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            duration_ms = duration_ms,
            "Execution span completed"
        );
    }

    fn on_error(&self, ctx: &InterceptorContext, error: &str, duration_ms: u64) {
        tracing::debug!(
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            duration_ms = duration_ms,
            error = %error,
            "Execution span errored"
        );
    }
}
