//! The trait every task implements.

use crate::error::Result;
use crate::task::TaskContext;

/// A task definition: a named unit of work a workflow can schedule.
///
/// Unlike workflows, tasks may have side effects and perform I/O. Tasks are
/// usually defined with the `#[task]` macro, which generates this
/// implementation; implement it by hand only when you need to.
///
/// # Example
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// struct ProcessPaymentTask;
///
/// #[async_trait]
/// impl Task for ProcessPaymentTask {
///     fn name() -> &'static str {
///         "ProcessPayment"
///     }
///
///     async fn execute(ctx: &TaskContext, input: Vec<u8>) -> Result<Vec<u8>> {
///         let order_id: String = serde_json::from_slice(&input)?;
///
///         ctx.heartbeat().await?;
///
///         // Tasks may do I/O, call databases, and so on.
///         let payment_id = process_payment(&order_id).await?;
///
///         let output = serde_json::to_vec(&payment_id)?;
///         Ok(output)
///     }
/// }
/// # async fn process_payment(order_id: &str) -> Result<String> {
/// #     Ok(format!("payment-{}", order_id))
/// # }
/// ```
#[async_trait::async_trait]
pub trait Task: Send + Sync + 'static {
    /// The task type name, which workflows use to schedule the task.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Run the task on its serialized input (typically JSON) and return its
    /// serialized output.
    ///
    /// # Errors
    ///
    /// Returns an error if the task fails. The task is then retried according
    /// to its retry policy.
    async fn execute(ctx: &TaskContext, input: Vec<u8>) -> Result<Vec<u8>>;
}
