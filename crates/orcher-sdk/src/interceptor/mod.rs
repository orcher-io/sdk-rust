//! Interceptor framework for cross-cutting concerns.
//!
//! Provides `InterceptorChain` for managing interceptors, and built-in
//! interceptors for logging, metrics, and tracing.
//!
//! ## Usage
//!
//! ```rust,no_run
//! # use orcher_sdk::prelude::*;
//! # use orcher_sdk::interceptor::{InterceptorChain, LoggingInterceptor, MetricsInterceptor, TracingInterceptor};
//! # async fn example() -> Result<()> {
//! let service = Worker::builder()
//!     .server_url("http://localhost:50051")
//!     .task_queue("orders")
//!     .interceptor(LoggingInterceptor::new())
//!     .interceptor(MetricsInterceptor::new())
//!     .interceptor(TracingInterceptor::new())
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod chain;
mod logging;
mod metrics;
mod tracing_interceptor;

pub use chain::InterceptorChain;
pub use logging::LoggingInterceptor;
pub use metrics::MetricsInterceptor;
pub use tracing_interceptor::TracingInterceptor;

pub use orcher_sdk_core::interceptor::{InterceptorContext, InterceptorHook, OperationType};
