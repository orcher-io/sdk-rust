//! Built-in logging interceptor.

use orcher_sdk_core::interceptor::{InterceptorContext, InterceptorHook};

/// Logs lifecycle events for every workflow and task execution.
///
/// Events go through the `tracing` crate: `INFO` for starts and completions,
/// `ERROR` for failures.
///
/// ## Example
///
/// ```rust
/// # use orcher_sdk::interceptor::{InterceptorChain, LoggingInterceptor};
/// let mut chain = InterceptorChain::new();
/// chain.add(LoggingInterceptor::new());
/// ```
pub struct LoggingInterceptor {
    _private: (),
}

impl LoggingInterceptor {
    /// Create a new logging interceptor.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for LoggingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl InterceptorHook for LoggingInterceptor {
    fn name(&self) -> &str {
        "logging"
    }

    fn before_execution(&self, ctx: &InterceptorContext) {
        tracing::info!(
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            name = %ctx.operation_name,
            attempt = ctx.attempt,
            task_queue = %ctx.task_queue,
            "Starting execution"
        );
    }

    fn after_execution(&self, ctx: &InterceptorContext, duration_ms: u64) {
        tracing::info!(
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            name = %ctx.operation_name,
            attempt = ctx.attempt,
            duration_ms = duration_ms,
            "Execution completed"
        );
    }

    fn on_error(&self, ctx: &InterceptorContext, error: &str, duration_ms: u64) {
        tracing::error!(
            operation = %ctx.operation_type,
            id = %ctx.operation_id,
            name = %ctx.operation_name,
            attempt = ctx.attempt,
            duration_ms = duration_ms,
            error = %error,
            "Execution failed"
        );
    }
}
