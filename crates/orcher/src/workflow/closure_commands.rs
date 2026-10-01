//! Turns inline closure results into `RecordStepResult` commands.
//!
//! Closures passed to `ctx.execute` run inside the workflow process, so the
//! engine never sees their results unless the SDK reports them. When a workflow
//! run finishes, the worker collects each closure result from the context and
//! sends it alongside the other commands. The engine journals it, and on replay
//! the closure returns the journaled result instead of running again.
//!
//! ```text
//! Workflow run:
//!   ctx.execute("fetch_user", closure).await?    → result kept in pending_task_results
//!   ctx.execute("query_orders", closure).await?  → result kept in pending_task_results
//!   return Ok(output)
//!
//! Worker:
//!   extract_closure_commands(&ctx)              → Vec<RecordStepResultCommand>
//!
//! Sent to the engine:
//!   CompleteWorkflowExecution {
//!     commands: [
//!       RecordStepResult { fetch_user result },
//!       RecordStepResult { query_orders result },
//!       CompleteWorkflow { workflow output }
//!     ]
//!   }
//!
//! Engine:
//!   records each step result in the journal
//! ```
//!
//! ## Usage
//!
//! ```rust
//! use orcher::prelude::*;
//! use orcher::workflow::closure_commands::extract_closure_commands;
//!
//! async fn execute_workflow(ctx: WorkflowContext) -> Result<(String, Vec<u32>)> {
//!     let user: String = ctx.execute("fetch_user", || async { Ok("ada".to_string()) }).await?;
//!     let orders: Vec<u32> = ctx.execute("query_orders", || async { Ok(vec![1, 2]) }).await?;
//!
//!     // The worker runtime does this itself when the workflow completes.
//!     let closure_commands = extract_closure_commands(&ctx)?;
//!     assert_eq!(closure_commands.len(), 2);
//!
//!     Ok((user, orders))
//! }
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! # let execution = orcher::workflow::WorkflowExecution::new(
//! #     "wf-1".into(), "run-1".into(), "example".into(), "default".into(), "queue".into());
//! # let ctx = WorkflowContext::new(execution, false, "1.0.0".into());
//! # execute_workflow(ctx).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::{Error, Result};
use crate::workflow::command::RecordStepResultCommand;
use crate::workflow::WorkflowContext;
use orcher_proto::orcher::v1::StepType;
use std::collections::HashMap;

/// Collects the successful closure results in `ctx` as `RecordStepResult` commands.
///
/// The worker runtime calls this when a workflow run completes, so the engine
/// can journal the results and replay them on a later run. Only steps recorded
/// as closures are returned; task results already reach the engine through
/// task completion. Steps with a missing or unknown step type are skipped
/// with a warning rather than recorded under the wrong type. The order of the
/// returned commands is not specified.
///
/// # Errors
///
/// Returns a state error if the workflow state lock is poisoned.
///
/// # Example
///
/// ```rust
/// # use orcher::prelude::*;
/// # use orcher::workflow::closure_commands::extract_closure_commands;
/// # async fn example(ctx: WorkflowContext) -> Result<()> {
/// let commands = extract_closure_commands(&ctx)?;
///
/// // One command per closure, for example:
/// // - RecordStepResult { step_name: "fetch_user_1", result: {...} }
/// // - RecordStepResult { step_name: "query_orders_2", result: {...} }
/// # Ok(())
/// # }
/// ```
pub fn extract_closure_commands(ctx: &WorkflowContext) -> Result<Vec<RecordStepResultCommand>> {
    let (pending_results, step_type_map, step_attempt_counts) = {
        let state = ctx.state.lock().map_err(|e| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "Failed to lock workflow state: {}",
                e
            )))
        })?;
        (
            state.pending_task_results.clone(),
            state.step_type_map.clone(),
            state.step_attempt_counts.clone(),
        )
    };

    let mut commands = Vec::new();

    for (step_id, result_bytes) in pending_results {
        // step_type_map: 0 = task, 1 = closure. Task results reach the engine
        // through task completion, so only closures are recorded here.
        let step_type = step_type_map.get(&step_id).copied();

        match step_type {
            Some(0) => {
                tracing::trace!(
                    step_id = %step_id,
                    "Skipping task step (handled via CompleteTaskExecution RPC)"
                );
                continue;
            }
            Some(1) => {
                tracing::debug!(
                    step_id = %step_id,
                    result_size = result_bytes.len(),
                    "Extracting closure result for server persistence"
                );
            }
            Some(_) => {
                tracing::warn!(
                    step_id = %step_id,
                    step_type = ?step_type,
                    "Unknown step_type value - skipping to avoid incorrect recording"
                );
                continue;
            }
            None => {
                // Every step records its type, so this indicates a bug. Skip rather
                // than record the step under the wrong type.
                tracing::warn!(
                    step_id = %step_id,
                    "Step has no type information in step_type_map - skipping to avoid incorrect recording"
                );
                continue;
            }
        }

        let execution_attempt = step_attempt_counts.get(&step_id).copied().unwrap_or(1);

        // `failure` is always None: `pending_task_results` holds only successful
        // closures; failed ones are kept in `pending_task_failures`.
        let command = RecordStepResultCommand {
            step_name: step_id.clone(),
            step_type: StepType::Closure as i32,
            result: result_bytes,
            failure: None,
            execution_attempt,
        };

        commands.push(command);
    }

    tracing::info!(
        closure_count = commands.len(),
        "Extracted closure commands for server persistence"
    );

    Ok(commands)
}

/// Guesses from its shape whether a step id belongs to a closure.
///
/// Returns `true` for ids of the form `{step_name}_{sequence_number}`. This is
/// a heuristic only: task step ids use the same shape, so it cannot tell a
/// closure from a task. [`extract_closure_commands`] uses the recorded step
/// type instead.
///
/// # Examples
///
/// ```rust
/// # use orcher::workflow::closure_commands::is_closure_step;
/// // Closure step ids look like step_name_N
/// assert_eq!(is_closure_step("fetch_user_1"), true);
/// assert_eq!(is_closure_step("query_orders_2"), true);
///
/// // Ids without a numeric suffix are not treated as closures
/// assert_eq!(is_closure_step("SendEmail::send_welcome"), false);
/// ```
pub fn is_closure_step(step_id: &str) -> bool {
    // Closure step ids end in `_{sequence}`, the number from ctx.next_sequence().
    if let Some(last_underscore_pos) = step_id.rfind('_') {
        let potential_sequence = &step_id[last_underscore_pos + 1..];
        if !potential_sequence.is_empty() && potential_sequence.chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }

    // Task type names are often module paths such as "ModuleName::task_name".
    if step_id.contains("::") {
        return false;
    }

    false
}

/// Collects the failed closure executions in `ctx` as `RecordStepResult` commands.
///
/// Each command carries the closure's failure and attempt count, with an empty
/// result.
///
/// # Errors
///
/// Returns a state error if the workflow state lock is poisoned.
pub fn extract_failed_closure_commands(
    ctx: &WorkflowContext,
) -> Result<Vec<RecordStepResultCommand>> {
    let (pending_failures, step_attempt_counts): (
        HashMap<String, orcher_proto::orcher::v1::Failure>,
        HashMap<String, i32>,
    ) = {
        let state = ctx.state.lock().map_err(|e| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "Failed to lock workflow state: {}",
                e
            )))
        })?;
        (
            state.pending_task_failures.clone(),
            state.step_attempt_counts.clone(),
        )
    };

    let mut commands = Vec::new();

    for (step_id, failure) in pending_failures {
        let execution_attempt = step_attempt_counts.get(&step_id).copied().unwrap_or(1);

        tracing::debug!(
            step_id = %step_id,
            attempt = execution_attempt,
            error = %failure.message,
            "Extracting failed closure command"
        );

        commands.push(RecordStepResultCommand {
            step_name: step_id,
            step_type: StepType::Closure as i32,
            result: vec![],
            failure: Some(failure),
            execution_attempt,
        });
    }

    tracing::debug!(
        failed_count = commands.len(),
        "Extracted failed closure commands"
    );

    Ok(commands)
}

/// Counts the closure and task results held in `ctx`, for debugging.
///
/// Steps are classified with [`is_closure_step`], so the split between
/// closures and tasks is approximate.
///
/// # Errors
///
/// Returns a state error if the workflow state lock is poisoned.
///
/// # Example
///
/// ```rust
/// # use orcher::prelude::*;
/// # use orcher::workflow::closure_commands::get_closure_stats;
/// # fn example(ctx: &WorkflowContext) -> Result<()> {
/// let stats = get_closure_stats(ctx)?;
/// println!("Executed {} closures, {} results pending persistence",
///          stats.total_executions, stats.results_pending);
/// # Ok(())
/// # }
/// ```
pub fn get_closure_stats(ctx: &WorkflowContext) -> Result<ClosureExecutionStats> {
    let state = ctx.state.lock().map_err(|e| {
        Error::Workflow(crate::error::WorkflowError::StateError(format!(
            "Failed to lock workflow state: {}",
            e
        )))
    })?;

    let total_results = state.pending_task_results.len();
    let closure_results = state
        .pending_task_results
        .keys()
        .filter(|k| is_closure_step(k))
        .count();

    Ok(ClosureExecutionStats {
        total_executions: closure_results,
        results_pending: closure_results,
        task_executions: total_results - closure_results,
    })
}

/// Statistics about closure executions in a workflow.
#[derive(Debug, Clone)]
pub struct ClosureExecutionStats {
    /// Number of closure executions.
    pub total_executions: usize,

    /// Number of closure results not yet sent to the engine.
    pub results_pending: usize,

    /// Number of task executions.
    pub task_executions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_closure_step() {
        assert!(is_closure_step("fetch_user_1"));
        assert!(is_closure_step("query_orders_2"));
        assert!(is_closure_step("external_api_call_10"));

        assert!(!is_closure_step("EmailService::send_welcome"));
        assert!(!is_closure_step("PaymentService::charge_card"));

        // Edge cases
        assert!(is_closure_step("step_123"));
        assert!(is_closure_step("_999")); // an empty step name still matches
    }

    #[test]
    fn test_closure_step_format() {
        assert!(is_closure_step("a_1"));
        assert!(is_closure_step("my_step_name_42"));

        assert!(!is_closure_step("no_sequence_here"));
        assert!(!is_closure_step("Task::execute"));
    }
}
