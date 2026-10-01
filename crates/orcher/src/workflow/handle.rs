//! Handle a parent workflow uses to wait on a child workflow it started.

use crate::error::{Error, Result};
use crate::workflow::context::ContextState;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::{Arc, Mutex};

/// Handle to a child workflow started without waiting for its result.
///
/// Use [`ChildWorkflowHandle::result`] to wait for the child. Sending events,
/// querying and canceling through the handle are not supported: those methods
/// always return an error.
///
/// ## Example
///
/// ```rust
/// use orcher::prelude::*;
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderResult {
///     status: String,
/// }
///
/// #[workflow]
/// async fn parent_workflow(ctx: WorkflowContext, order_id: String) -> Result<String> {
///     // Start the child without waiting for it
///     let child = ctx.start_child_workflow("process_order", order_id).await?;
///
///     // Do other work meanwhile
///     ctx.sleep(Duration::from_secs(60)).await?;
///
///     // Then wait for the child's result
///     let result: OrderResult = child.result().await?;
///
///     Ok(result.status)
/// }
/// ```
#[derive(Clone)]
pub struct ChildWorkflowHandle {
    workflow_id: String,

    run_id: String,

    workflow_type: String,

    /// The parent's context state. The runtime injects the child's completion
    /// into `pending_task_results` under `child:{workflow_id}`, the same entry
    /// the execute path reads, so the handle keeps no result state of its own.
    ctx_state: Arc<Mutex<ContextState>>,
}

impl std::fmt::Debug for ChildWorkflowHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildWorkflowHandle")
            .field("workflow_id", &self.workflow_id)
            .field("run_id", &self.run_id)
            .field("workflow_type", &self.workflow_type)
            .finish_non_exhaustive()
    }
}

impl ChildWorkflowHandle {
    /// Creates a handle; the SDK calls this when it starts a child workflow.
    pub(crate) fn new(
        workflow_id: String,
        run_id: String,
        workflow_type: String,
        ctx_state: Arc<Mutex<ContextState>>,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            workflow_type,
            ctx_state,
        }
    }

    /// Returns the child workflow ID.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(child: ChildWorkflowHandle) {
    /// let workflow_id = child.id();
    /// println!("Child workflow ID: {}", workflow_id);
    /// # }
    /// ```
    pub fn id(&self) -> &str {
        &self.workflow_id
    }

    /// Returns the child's run ID, which is unique to each run of the workflow.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns the child workflow type.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Waits for the child workflow to finish and returns its result.
    ///
    /// Until the child finishes, this returns a suspension error; the runtime
    /// then waits for the child and replays the workflow, and on replay the
    /// result is available.
    ///
    /// # Errors
    ///
    /// Returns `ChildWorkflowFailed` if the child did not complete successfully,
    /// and a serialization error if the result does not deserialize into `O`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// #[derive(Deserialize)]
    /// struct OrderResult {
    ///     order_id: String,
    /// }
    ///
    /// # async fn example(ctx: &WorkflowContext, input: String) -> Result<()> {
    /// let child = ctx.start_child_workflow("process_order", input).await?;
    ///
    /// let result: OrderResult = child.result().await?;
    /// tracing::info!(order_id = %result.order_id, "order processed");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn result<O: DeserializeOwned>(&self) -> Result<O> {
        // The runtime stores the child's completion under `child:{id}`: the
        // result bytes on success, or a `{"__orcher_child_failed__":true,
        // "message":...}` sentinel on failure. The execute path reads the same entry.
        let cached = {
            let state = self.ctx_state.lock().unwrap();
            let key = format!("child:{}", self.workflow_id);
            let cached = state.pending_task_results.get(&key).cloned();
            if cached.is_some() {
                // Received: the workflow's clock moves to when it was journaled.
                state.observe(&key);
            }
            cached
        };

        let Some(result_bytes) = cached else {
            // Not finished yet: suspend so the runtime waits for the child.
            return Err(Error::Workflow(crate::error::WorkflowError::Suspended {
                reason: format!("Waiting for child workflow {}", self.workflow_id),
                pending_operations: vec![self.workflow_id.clone()],
            }));
        };

        // Turn the failure sentinel into an error the workflow can catch.
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&result_bytes) {
            if v.get("__orcher_child_failed__")
                .and_then(|b| b.as_bool())
                .unwrap_or(false)
            {
                let msg = v
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("child workflow failed");
                return Err(Error::Workflow(
                    crate::error::WorkflowError::ChildWorkflowFailed {
                        workflow_type: self.workflow_type.clone(),
                        workflow_id: self.workflow_id.clone(),
                        reason: msg.to_string(),
                    },
                ));
            }
        }

        serde_json::from_slice(&result_bytes).map_err(|e| {
            Error::Serialization(format!(
                "Failed to deserialize child workflow result: {}",
                e
            ))
        })
    }

    /// Sends an event to the child workflow.
    ///
    /// Not supported: the event is not delivered, and the call only logs it.
    ///
    /// # Errors
    ///
    /// Always returns an error: a serialization error if `arg` cannot be
    /// serialized, otherwise a state error.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher::prelude::*;
    /// # use serde::{Serialize, Deserialize};
    /// # async fn example(child: ChildWorkflowHandle) -> Result<()> {
    /// #[derive(Serialize, Deserialize)]
    /// struct ApprovalData {
    ///     approved: bool,
    ///     approver: String,
    /// }
    ///
    /// child.send_event("approve_order", ApprovalData {
    ///     approved: true,
    ///     approver: "manager@example.com".to_string(),
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_event<E: Serialize>(&self, event_name: &str, arg: E) -> Result<()> {
        let _payload = serde_json::to_vec(&arg).map_err(|e| {
            Error::Serialization(format!("Failed to serialize event payload: {}", e))
        })?;

        tracing::info!(
            workflow_id = %self.workflow_id,
            event_name = %event_name,
            "Sending event to child workflow"
        );

        Err(Error::Workflow(crate::error::WorkflowError::StateError(
            format!(
                "Sending events to child workflow {} requires execution context - will be implemented with executor integration",
                self.workflow_id
            )
        )))
    }

    /// Queries the child workflow's state.
    ///
    /// Not supported: the query is not sent, and the call only logs it.
    ///
    /// # Errors
    ///
    /// Always returns an error: a serialization error if `arg` cannot be
    /// serialized, otherwise a state error.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher::prelude::*;
    /// # async fn example(child: ChildWorkflowHandle) -> Result<()> {
    /// let status: String = child.query("get_status", ()).await?;
    /// println!("Child workflow status: {}", status);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query<Q: Serialize, R: DeserializeOwned>(
        &self,
        query_name: &str,
        arg: Q,
    ) -> Result<R> {
        let _arg_bytes = serde_json::to_vec(&arg).map_err(|e| {
            Error::Serialization(format!("Failed to serialize query argument: {}", e))
        })?;

        tracing::info!(
            workflow_id = %self.workflow_id,
            query_name = %query_name,
            "Querying child workflow"
        );

        Err(Error::Workflow(crate::error::WorkflowError::StateError(
            format!(
                "Querying child workflow {} - will be fully implemented in Task 3.2",
                self.workflow_id
            ),
        )))
    }

    /// Requests cancellation of the child workflow.
    ///
    /// Not supported: no cancellation is sent, and the call only logs it.
    ///
    /// # Errors
    ///
    /// Always returns a state error.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use orcher::prelude::*;
    /// # async fn example(child: ChildWorkflowHandle) -> Result<()> {
    /// child.cancel().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel(&self) -> Result<()> {
        tracing::info!(
            workflow_id = %self.workflow_id,
            "Canceling child workflow"
        );

        Err(Error::Workflow(crate::error::WorkflowError::StateError(
            format!(
                "Canceling child workflow {} - will be implemented with executor integration",
                self.workflow_id
            ),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{WorkflowContext, WorkflowExecution};

    /// Builds a handle on a fresh context's state, so `result()` reads the same
    /// `child:{id}` entry the runtime writes.
    fn make_handle(child_id: &str) -> (WorkflowContext, ChildWorkflowHandle) {
        let execution = WorkflowExecution {
            workflow_id: "parent-wf".to_string(),
            run_id: "parent-run".to_string(),
            workflow_type: "ParentWorkflow".to_string(),
            attempt: 1,
            namespace: "default".to_string(),
            task_queue: "test-queue".to_string(),
        };
        let ctx = WorkflowContext::new(execution, false, "1.0".to_string());
        let handle = ChildWorkflowHandle::new(
            child_id.to_string(),
            "run-456".to_string(),
            "OrderWorkflow".to_string(),
            Arc::clone(&ctx.state),
        );
        (ctx, handle)
    }

    /// Stores a child completion the way the runtime does on a
    /// ChildWorkflowCompleted or ChildWorkflowFailed job.
    fn inject_child(ctx: &WorkflowContext, child_id: &str, bytes: Vec<u8>) {
        ctx.state
            .lock()
            .unwrap()
            .pending_task_results
            .insert(format!("child:{}", child_id), bytes);
    }

    #[test]
    fn test_child_workflow_handle_creation() {
        let (_ctx, handle) = make_handle("child-wf-123");

        assert_eq!(handle.id(), "child-wf-123");
        assert_eq!(handle.run_id(), "run-456");
        assert_eq!(handle.workflow_type(), "OrderWorkflow");
    }

    #[tokio::test]
    async fn test_child_workflow_result_not_ready() {
        let (_ctx, handle) = make_handle("child-wf-123");

        // No completion stored yet, so the call suspends.
        let result: Result<String> = handle.result().await;
        assert!(matches!(
            result,
            Err(Error::Workflow(
                crate::error::WorkflowError::Suspended { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_child_workflow_result_from_cache() {
        let (ctx, handle) = make_handle("child-wf-123");
        inject_child(
            &ctx,
            "child-wf-123",
            serde_json::to_vec(&"success".to_string()).unwrap(),
        );

        let result: String = handle.result().await.unwrap();
        assert_eq!(result, "success");
    }

    #[tokio::test]
    async fn test_child_workflow_failure_sentinel() {
        let (ctx, handle) = make_handle("child-wf-123");
        // The runtime stores a failed child as this sentinel.
        let sentinel = serde_json::json!({
            "__orcher_child_failed__": true,
            "message": "workflow failed",
        });
        inject_child(&ctx, "child-wf-123", serde_json::to_vec(&sentinel).unwrap());

        let result: Result<String> = handle.result().await;
        if let Err(Error::Workflow(crate::error::WorkflowError::ChildWorkflowFailed {
            workflow_type,
            workflow_id,
            reason,
        })) = result
        {
            assert_eq!(workflow_type, "OrderWorkflow");
            assert_eq!(workflow_id, "child-wf-123");
            assert_eq!(reason, "workflow failed");
        } else {
            panic!("Expected ChildWorkflowFailed error");
        }
    }

    #[tokio::test]
    async fn test_child_workflow_result_deserialization() {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct OrderResult {
            order_id: String,
            total: f64,
        }

        let (ctx, handle) = make_handle("child-wf-123");
        let order_result = OrderResult {
            order_id: "order-999".to_string(),
            total: 99.99,
        };
        inject_child(
            &ctx,
            "child-wf-123",
            serde_json::to_vec(&order_result).unwrap(),
        );

        let result: OrderResult = handle.result().await.unwrap();
        assert_eq!(result, order_result);
    }

    #[tokio::test]
    async fn test_child_workflow_send_event() {
        let (_ctx, handle) = make_handle("child-wf-123");

        // Not supported, so always an error.
        let result = handle.send_event("approve", true).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_child_workflow_query() {
        let (_ctx, handle) = make_handle("child-wf-123");

        // Not supported, so always an error.
        let result: Result<String> = handle.query("get_status", ()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_child_workflow_cancel() {
        let (_ctx, handle) = make_handle("child-wf-123");

        // Not supported, so always an error.
        let result = handle.cancel().await;
        assert!(result.is_err());
    }
}
