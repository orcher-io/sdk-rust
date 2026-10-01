//! Interceptor chain that manages and invokes multiple interceptors.

use orcher_sdk_core::interceptor::{InterceptorContext, InterceptorHook};
use std::sync::Arc;

/// A chain of interceptors invoked in order for each execution.
///
/// The chain calls `before_execution` on all interceptors in insertion order,
/// then `after_execution` or `on_error` in reverse order, so the first
/// interceptor added wraps all the others.
#[derive(Clone)]
pub struct InterceptorChain {
    interceptors: Vec<Arc<dyn InterceptorHook>>,
}

impl InterceptorChain {
    /// Create an empty interceptor chain.
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    /// Add an interceptor to the chain.
    pub fn add(&mut self, interceptor: impl InterceptorHook + 'static) {
        self.interceptors.push(Arc::new(interceptor));
    }

    /// Add a shared interceptor to the chain.
    pub fn add_shared(&mut self, interceptor: Arc<dyn InterceptorHook>) {
        self.interceptors.push(interceptor);
    }

    /// Returns `true` if the chain has no interceptors.
    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    /// Number of interceptors in the chain.
    pub fn len(&self) -> usize {
        self.interceptors.len()
    }

    /// Call `before_execution` on all interceptors (in order).
    pub fn before_execution(&self, ctx: &InterceptorContext) {
        for interceptor in &self.interceptors {
            interceptor.before_execution(ctx);
        }
    }

    /// Call `after_execution` on all interceptors (in reverse order).
    pub fn after_execution(&self, ctx: &InterceptorContext, duration_ms: u64) {
        for interceptor in self.interceptors.iter().rev() {
            interceptor.after_execution(ctx, duration_ms);
        }
    }

    /// Call `on_error` on all interceptors (in reverse order).
    pub fn on_error(&self, ctx: &InterceptorContext, error: &str, duration_ms: u64) {
        for interceptor in self.interceptors.iter().rev() {
            interceptor.on_error(ctx, error, duration_ms);
        }
    }
}

impl Default for InterceptorChain {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InterceptorChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.interceptors.iter().map(|i| i.name()).collect();
        f.debug_struct("InterceptorChain")
            .field("interceptors", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingInterceptor {
        name: &'static str,
        before_count: AtomicU32,
        after_count: AtomicU32,
        error_count: AtomicU32,
    }

    impl CountingInterceptor {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                before_count: AtomicU32::new(0),
                after_count: AtomicU32::new(0),
                error_count: AtomicU32::new(0),
            }
        }
    }

    impl InterceptorHook for CountingInterceptor {
        fn name(&self) -> &str {
            self.name
        }

        fn before_execution(&self, _ctx: &InterceptorContext) {
            self.before_count.fetch_add(1, Ordering::SeqCst);
        }

        fn after_execution(&self, _ctx: &InterceptorContext, _duration_ms: u64) {
            self.after_count.fetch_add(1, Ordering::SeqCst);
        }

        fn on_error(&self, _ctx: &InterceptorContext, _error: &str, _duration_ms: u64) {
            self.error_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_chain_empty() {
        let chain = InterceptorChain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
    }

    #[test]
    fn test_chain_calls_all_interceptors() {
        let i1 = Arc::new(CountingInterceptor::new("i1"));
        let i2 = Arc::new(CountingInterceptor::new("i2"));

        let mut chain = InterceptorChain::new();
        chain.add_shared(i1.clone());
        chain.add_shared(i2.clone());

        let ctx = InterceptorContext::workflow("wf-1", "Test", 1, "default", "q");

        chain.before_execution(&ctx);
        assert_eq!(i1.before_count.load(Ordering::SeqCst), 1);
        assert_eq!(i2.before_count.load(Ordering::SeqCst), 1);

        chain.after_execution(&ctx, 100);
        assert_eq!(i1.after_count.load(Ordering::SeqCst), 1);
        assert_eq!(i2.after_count.load(Ordering::SeqCst), 1);

        chain.on_error(&ctx, "test", 50);
        assert_eq!(i1.error_count.load(Ordering::SeqCst), 1);
        assert_eq!(i2.error_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_chain_debug() {
        let mut chain = InterceptorChain::new();
        chain.add(CountingInterceptor::new("logging"));
        chain.add(CountingInterceptor::new("metrics"));

        let debug = format!("{:?}", chain);
        assert!(debug.contains("logging"));
        assert!(debug.contains("metrics"));
    }
}
