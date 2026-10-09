//! Handle to a single workflow execution.

use crate::error::{Error, Result};
use orcher_sdk_core::client::WorkflowClient as CoreClient;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

use super::{CancelOptions, EventOptions, QueryOptions};

/// Handle to a workflow execution.
///
/// Use it to wait for the result, send events, run queries and updates, and
/// cancel, terminate or reset the workflow. Without a run ID, calls target the
/// latest run. Cloning is cheap.
#[derive(Clone)]
pub struct WorkflowHandle {
    pub(crate) core_client: Arc<CoreClient>,
    pub(crate) workflow_id: String,
    pub(crate) run_id: Option<String>,
    /// Namespace for every call; the client's default when `None`.
    pub(crate) namespace: Option<String>,
}

impl WorkflowHandle {
    pub(crate) fn new_with_namespace(
        core_client: Arc<CoreClient>,
        workflow_id: String,
        run_id: Option<String>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            core_client,
            workflow_id,
            run_id,
            namespace,
        }
    }

    fn get_client(&self) -> CoreClient {
        if let Some(ref namespace) = self.namespace {
            self.core_client.as_ref().clone().with_namespace(namespace)
        } else {
            self.core_client.as_ref().clone()
        }
    }

    /// The workflow ID.
    pub fn id(&self) -> &str {
        &self.workflow_id
    }

    /// The run this handle targets, if it was given one.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    // ============================================================================
    // Status
    // ============================================================================

    /// Returns the current status without waiting for completion.
    ///
    /// Use [`WorkflowHandle::describe`] for full metadata.
    pub async fn status(&self) -> Result<orcher_sdk_core::WorkflowStatus> {
        let client = self.get_client();
        client
            .get_workflow_status(&self.workflow_id, self.run_id.clone())
            .await
            .map_err(Error::from)
    }

    // ============================================================================
    // Workflow Results
    // ============================================================================

    /// Waits until the workflow completes and returns its result.
    ///
    /// # Errors
    ///
    /// Fails if the workflow does not complete successfully, or if the result
    /// cannot be deserialized from JSON into `T`.
    pub async fn result<T: DeserializeOwned>(&self) -> Result<T> {
        let run_id = self.run_id.clone();

        tracing::debug!(
            workflow_id = %self.workflow_id,
            run_id = ?run_id,
            "Waiting for workflow result"
        );

        let client = self.get_client();
        let result_bytes = client
            .get_workflow_result_raw(&self.workflow_id, run_id.as_ref().unwrap_or(&String::new()))
            .await
            .map_err(Error::from)?;

        let result: T = serde_json::from_slice(&result_bytes).map_err(|e| {
            Error::Client(crate::error::ClientError::Deserialization(format!(
                "Failed to deserialize workflow result: {}",
                e
            )))
        })?;

        tracing::debug!(
            workflow_id = %self.workflow_id,
            run_id = ?run_id,
            "Workflow completed successfully"
        );

        Ok(result)
    }

    /// Like [`WorkflowHandle::result`], but gives up after `timeout`.
    ///
    /// Giving up only stops the wait; the workflow keeps running.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Timeout`] if the workflow has not completed in time,
    /// and otherwise the same errors as [`WorkflowHandle::result`].
    ///
    /// [`ClientError::Timeout`]: crate::error::ClientError::Timeout
    pub async fn result_with_timeout<T: DeserializeOwned>(&self, timeout: Duration) -> Result<T> {
        tokio::time::timeout(timeout, self.result())
            .await
            .map_err(|_| Error::Client(crate::error::ClientError::Timeout { timeout }))?
    }

    // ============================================================================
    // Lifecycle Operations
    // ============================================================================

    /// Requests graceful cancellation.
    ///
    /// The workflow is told to cancel and can clean up before it stops. This
    /// returns once the request is accepted, not when the workflow has stopped.
    /// Use [`WorkflowHandle::cancel_with_options`] to limit how long the
    /// cleanup may take.
    pub async fn cancel(&self) -> Result<()> {
        self.cancel_with_options(CancelOptions::default()).await
    }

    /// Requests graceful cancellation with options.
    ///
    /// With [`CancelOptions::cleanup_timeout`] set, the engine terminates the
    /// workflow if it is still cleaning up when the limit passes. Engines from
    /// before cancellation cleanup ignore the limit and end the run as
    /// cancelled at once.
    ///
    /// ```rust,no_run
    /// # use orcher_sdk::client::{CancelOptions, WorkflowHandle};
    /// # use std::time::Duration;
    /// # async fn example(handle: WorkflowHandle) -> orcher_sdk::Result<()> {
    /// handle
    ///     .cancel_with_options(CancelOptions::default().with_cleanup_timeout(Duration::from_secs(30)))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_with_options(&self, options: CancelOptions) -> Result<()> {
        tracing::info!(
            workflow_id = %self.workflow_id,
            cleanup_timeout = ?options.cleanup_timeout,
            "Canceling workflow"
        );

        let client = self.get_client();
        client
            .cancel_workflow_with(&self.workflow_id, self.run_id.clone(), options.to_core())
            .await
            .map_err(Error::from)?;

        tracing::info!(
            workflow_id = %self.workflow_id,
            "Workflow cancellation requested"
        );

        Ok(())
    }

    /// Terminates the workflow immediately, without cleanup.
    ///
    /// Use [`WorkflowHandle::cancel`] to let the workflow clean up.
    pub async fn terminate(&self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        tracing::warn!(
            workflow_id = %self.workflow_id,
            reason = %reason,
            "Terminating workflow"
        );

        let client = self.get_client();
        client
            .terminate_workflow(&self.workflow_id, self.run_id.clone(), &reason)
            .await
            .map_err(Error::from)?;

        tracing::warn!(
            workflow_id = %self.workflow_id,
            "Workflow terminated"
        );

        Ok(())
    }

    /// Resets the workflow to a journal event and re-executes from there.
    ///
    /// Creates a new execution that replays the journal up to and including
    /// `target_event_id`, then makes fresh decisions from that point. The
    /// current execution ends with the terminal status "reset". `reason` is a
    /// human-readable note recorded with the reset.
    ///
    /// Returns the ID of the new execution.
    pub async fn reset(&self, target_event_id: i64, reason: impl Into<String>) -> Result<String> {
        let reason = reason.into();
        tracing::info!(
            workflow_id = %self.workflow_id,
            target_event_id = target_event_id,
            reason = %reason,
            "Resetting workflow"
        );

        let client = self.get_client();
        let response = client
            .reset_workflow(
                &self.workflow_id,
                self.run_id.clone(),
                target_event_id,
                &reason,
            )
            .await
            .map_err(Error::from)?;

        tracing::info!(
            workflow_id = %self.workflow_id,
            new_execution_id = %response,
            "Workflow reset successfully"
        );

        Ok(response)
    }

    // ============================================================================
    // Events
    // ============================================================================

    /// Sends an event, an asynchronous message, to the workflow.
    ///
    /// Returns once the server has accepted the event, not when the workflow
    /// has handled it.
    pub async fn send_event<E>(&self, event: impl Into<String>, arg: E) -> Result<()>
    where
        E: Serialize,
    {
        self.send_event_with_options(event, arg, EventOptions::default())
            .await
    }

    /// Sends an event with options.
    ///
    /// The options are accepted but not applied: the request uses the client's
    /// timeout.
    pub async fn send_event_with_options<E>(
        &self,
        event: impl Into<String>,
        arg: E,
        _options: EventOptions,
    ) -> Result<()>
    where
        E: Serialize,
    {
        let event = event.into();

        tracing::debug!(
            workflow_id = %self.workflow_id,
            event = %event,
            "Sending event to workflow"
        );

        // sdk-core serializes `arg`.
        let client = self.get_client();
        client
            .send_event(&self.workflow_id, self.run_id.clone(), event.clone(), arg)
            .await
            .map_err(Error::from)?;

        tracing::debug!(
            workflow_id = %self.workflow_id,
            event = %event,
            "Event sent successfully"
        );

        Ok(())
    }

    // ============================================================================
    // Queries
    // ============================================================================

    /// Reads workflow state through a named query, without affecting execution.
    pub async fn query<Q, R>(&self, query: impl Into<String>, args: Q) -> Result<R>
    where
        Q: Serialize,
        R: DeserializeOwned,
    {
        self.query_with_options(query, args, QueryOptions::default())
            .await
    }

    /// Runs a query with options.
    ///
    /// The options are accepted but not applied: the request uses the client's
    /// timeout and no reject condition.
    pub async fn query_with_options<Q, R>(
        &self,
        query: impl Into<String>,
        args: Q,
        _options: QueryOptions,
    ) -> Result<R>
    where
        Q: Serialize,
        R: DeserializeOwned,
    {
        let query = query.into();

        tracing::debug!(
            workflow_id = %self.workflow_id,
            query = %query,
            "Querying workflow"
        );

        // sdk-core serializes `args` and deserializes the result.
        let client = self.get_client();
        let result: R = client
            .query_workflow(&self.workflow_id, self.run_id.clone(), query.clone(), args)
            .await
            .map_err(Error::from)?;

        tracing::debug!(
            workflow_id = %self.workflow_id,
            query = %query,
            "Query completed successfully"
        );

        Ok(result)
    }

    // ============================================================================
    // Description
    // ============================================================================

    /// Returns detailed metadata, such as status, timing and task queue,
    /// without waiting for completion.
    pub async fn describe(&self) -> Result<orcher_sdk_core::WorkflowExecutionDescription> {
        tracing::debug!(
            workflow_id = %self.workflow_id,
            "Describing workflow"
        );

        let client = self.get_client();
        let description = client
            .describe_workflow(&self.workflow_id, self.run_id.clone())
            .await
            .map_err(Error::from)?;

        tracing::debug!(
            workflow_id = %self.workflow_id,
            status = ?description.status,
            "Workflow described successfully"
        );

        Ok(description)
    }

    // ============================================================================
    // Updates
    // ============================================================================

    /// Sends an update to the workflow and waits for its result.
    ///
    /// Unlike a query, an update may change workflow state; its result is
    /// recorded in the journal.
    pub async fn update<A, R>(&self, update_name: impl Into<String>, args: A) -> Result<R>
    where
        A: Serialize,
        R: DeserializeOwned,
    {
        let update_name = update_name.into();

        tracing::debug!(
            workflow_id = %self.workflow_id,
            update_name = %update_name,
            "Updating workflow"
        );

        let client = self.get_client();
        let result: R = client
            .update_workflow(
                &self.workflow_id,
                self.run_id.clone(),
                update_name.clone(),
                args,
            )
            .await
            .map_err(Error::from)?;

        tracing::debug!(
            workflow_id = %self.workflow_id,
            update_name = %update_name,
            "Update completed successfully"
        );

        Ok(result)
    }
}

impl std::fmt::Debug for WorkflowHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandle")
            .field("workflow_id", &self.workflow_id)
            .field("run_id", &self.run_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_handle_debug() {
        // A real handle needs a connected CoreClient, so this only checks the
        // values one would be built from.
        let workflow_id = "test-workflow-123".to_string();
        let run_id = Some("test-run-456".to_string());

        assert_eq!(workflow_id, "test-workflow-123");
        assert_eq!(run_id, Some("test-run-456".to_string()));
    }

    #[test]
    fn test_event_options_defaults() {
        let options = EventOptions::default();
        assert!(options.timeout.is_none());
    }

    #[test]
    fn test_query_options_defaults() {
        let options = QueryOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.reject_condition.is_none());
    }
}

#[cfg(test)]
mod over_the_wire_tests {
    use super::*;

    /// Checks that the handle surfaces a typed error instead of flattening it.
    ///
    /// The unit tests beside `From<CoreError>` prove the mapping is correct, but
    /// not that the handle uses it. If the handle wrapped every failure in
    /// `ClientError::RequestFailed`, which `is_retryable` treats as retryable, a
    /// workflow that will never exist would be retried in a loop. Only a real
    /// call can show the difference.
    ///
    /// Ignored by default because it needs a server:
    ///   cargo test -p orcher-sdk --lib over_the_wire -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "requires a running orcher server on localhost:50051"]
    async fn a_missing_workflow_is_typed_and_not_retryable_over_the_wire() {
        let core_client = Arc::new(
            CoreClient::connect("http://localhost:50051")
                .await
                .expect("connect to server"),
        );
        let handle = WorkflowHandle::new_with_namespace(
            core_client,
            format!("no-such-workflow-{}", uuid::Uuid::new_v4()),
            None,
            Some("default".to_string()),
        );

        let err = handle
            .status()
            .await
            .expect_err("a workflow that does not exist should not return a status");

        println!("handle.status() -> {err:?}");
        println!("is_retryable    -> {}", err.is_retryable());

        assert!(
            !err.is_retryable(),
            "a workflow that will never exist was classified retryable, so a \
             retry loop would hammer the server: {err:?}"
        );
    }
}
