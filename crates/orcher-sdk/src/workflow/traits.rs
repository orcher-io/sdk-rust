//! The [`Workflow`] trait.

use crate::error::Result;
use crate::workflow::WorkflowContext;

/// A workflow definition.
///
/// A workflow is a durable function that orchestrates tasks and other workflows. Side
/// effects belong in tasks, not in the workflow body.
///
/// # Determinism
///
/// A workflow is replayed from its history, so it must be deterministic:
/// - no direct I/O;
/// - no random numbers;
/// - no system clock (use workflow timers and time APIs instead);
/// - no threads or concurrency outside the workflow APIs.
///
/// Most workflows are written with the `#[workflow]` macro, which generates this impl.
///
/// # Example
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// struct OrderWorkflow;
///
/// #[async_trait]
/// impl Workflow for OrderWorkflow {
///     fn name() -> &'static str {
///         "OrderWorkflow"
///     }
///
///     async fn execute(ctx: &mut WorkflowContext, input: Vec<u8>) -> Result<Vec<u8>> {
///         let order_id: String = serde_json::from_slice(&input)?;
///
///         let payment_id: String = ctx.execute_task("process_payment", order_id).await?;
///
///         // A durable timer: the wait survives worker restarts.
///         ctx.sleep(std::time::Duration::from_secs(300)).await?;
///
///         let tracking: String = ctx.execute_task("ship_order", payment_id).await?;
///
///         let output = serde_json::to_vec(&tracking)?;
///         Ok(output)
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Workflow: Send + Sync + 'static {
    /// The workflow type name. Clients use it to start workflows of this type.
    fn name() -> &'static str
    where
        Self: Sized;

    /// Runs the workflow on serialized (typically JSON) input and returns serialized output.
    ///
    /// On replay this is called again with the same input and must produce the same
    /// sequence of commands.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow fails. The workflow is then retried according to
    /// the orchestrator's retry policy.
    async fn execute(ctx: &mut WorkflowContext, input: Vec<u8>) -> Result<Vec<u8>>;
}
