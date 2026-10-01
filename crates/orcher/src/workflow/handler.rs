//! Workflow handler types.
//!
//! The worker's `ExecutionRuntime` stores registered handlers and runs them. Polling and
//! communication with the server go through sdk-core's `WorkflowDriver`.

use crate::error::Result;
use crate::workflow::WorkflowContext;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The signature of a workflow function: an async function that takes a
/// [`WorkflowContext`] and returns a result.
///
/// # Example
///
/// ```rust
/// use orcher::prelude::*;
/// use orcher::workflow::handler::UserWorkflowFn;
/// use std::sync::Arc;
///
/// async fn my_workflow(ctx: WorkflowContext) -> Result<String> {
///     let charge_id: String = ctx.execute_task("charge_card", 4_200).await?;
///     Ok(charge_id)
/// }
///
/// let handler: UserWorkflowFn<String> = Arc::new(|ctx| Box::pin(my_workflow(ctx)));
/// ```
pub type UserWorkflowFn<T> =
    Arc<dyn Fn(WorkflowContext) -> Pin<Box<dyn Future<Output = Result<T>> + Send>> + Send + Sync>;
