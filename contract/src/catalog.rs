//! The workflows, tasks and actors the contract worker serves.
//!
//! Every SDK's contract worker implements this same set of named workflows and
//! tasks. Each workflow returns a JSON result describing the durable behavior it
//! observed, so the harness asserts on a client-observable outcome and never
//! inspects engine internals. Keep names and result shapes in step with the
//! other SDKs and with `scenarios.json`.

use orcher::error::{Error, TaskError, WorkflowError};
use orcher::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct EchoInput {
    pub message: String,
}

/// Returns its input unchanged. Proves the worker runs and a workflow completes
/// with a result.
#[workflow(name = "echo")]
pub async fn echo(_ctx: WorkflowContext, input: EchoInput) -> Result<EchoInput> {
    Ok(input)
}

/// Empty input for workflows that take no meaningful arguments (deserializes from `{}`).
#[derive(Serialize, Deserialize, Default)]
pub struct Empty {}

/// Result of catching a durable task failure.
#[derive(Serialize, Deserialize)]
pub struct CatchResult {
    pub caught: bool,
    pub task_type: String,
    pub attempts: u32,
}

/// A task that fails on its only attempt. Exercises the durable task-failure
/// path.
#[task(retry = 1)]
pub async fn always_fail(_ctx: TaskContext, _input: Empty) -> Result<Empty> {
    Err(Error::Task(TaskError::execution_failed(
        "intentional contract failure",
    )))
}

/// A task that always fails, declared with three attempts so the engine retries it
/// to exhaustion. Used to check the reported attempt count, not just that a failure
/// is surfaced.
#[task(retry = 3)]
pub async fn always_fail_retried(_ctx: TaskContext, _input: Empty) -> Result<Empty> {
    Err(Error::Task(TaskError::execution_failed(
        "intentional contract failure (retried)",
    )))
}

/// Runs a task until its retries are exhausted and reports how many attempts the
/// failure says it took. Guards the attempt count specifically: a worker that drops
/// it still reports a failure, and every other assertion here still passes, so
/// nothing else in this suite would notice.
#[workflow(name = "task_retries_exhausted")]
pub async fn task_retries_exhausted(ctx: WorkflowContext, _input: Empty) -> Result<CatchResult> {
    let outcome: Result<Empty> = ctx
        .execute_task(always_fail_retried, Empty::default())
        .await;
    match outcome {
        Ok(_) => Ok(CatchResult {
            caught: false,
            task_type: String::new(),
            attempts: 0,
        }),
        Err(Error::Workflow(WorkflowError::TaskFailed {
            task_type,
            attempts,
            ..
        })) => Ok(CatchResult {
            caught: true,
            task_type,
            attempts,
        }),
        Err(other) => Err(other),
    }
}

/// Runs `task` to its durable outcome and reports whether it failed, and after how
/// many attempts.
async fn attempts_until_failure<T: orcher::task::IntoTaskName>(
    ctx: &WorkflowContext,
    task: T,
) -> Result<CatchResult> {
    let outcome: Result<Empty> = ctx.execute_task(task, Empty::default()).await;
    match outcome {
        Ok(_) => Ok(CatchResult {
            caught: false,
            task_type: String::new(),
            attempts: 0,
        }),
        Err(Error::Workflow(WorkflowError::TaskFailed {
            task_type,
            attempts,
            ..
        })) => Ok(CatchResult {
            caught: true,
            task_type,
            attempts,
        }),
        Err(other) => Err(other),
    }
}

/// Fails with a type its retry policy lists as non-retryable, so it runs once.
#[task(retry = 3, non_retryable_errors = ["CardDeclined"])]
pub async fn decline_card(_ctx: TaskContext, _input: Empty) -> Result<Empty> {
    Err(TaskError::application("CardDeclined", "card declined").into())
}

/// Fails with a type its policy does not list, so it is retried to exhaustion.
#[task(retry = 3, non_retryable_errors = ["CardDeclined"])]
pub async fn throttle(_ctx: TaskContext, _input: Empty) -> Result<Empty> {
    Err(TaskError::application("Throttled", "slow down").into())
}

/// Fails marked non-retryable under a type its policy does not list, so it runs
/// once: the mark alone decides.
#[task(retry = 3)]
pub async fn close_account(_ctx: TaskContext, _input: Empty) -> Result<Empty> {
    Err(TaskError::non_retryable("AccountClosed", "account closed").into())
}

/// A failure whose type the policy lists as non-retryable is not retried.
#[workflow(name = "task_non_retryable_listed")]
pub async fn task_non_retryable_listed(ctx: WorkflowContext, _input: Empty) -> Result<CatchResult> {
    attempts_until_failure(&ctx, decline_card).await
}

/// A failure raised as non-retryable is not retried.
#[workflow(name = "task_non_retryable_marked")]
pub async fn task_non_retryable_marked(ctx: WorkflowContext, _input: Empty) -> Result<CatchResult> {
    attempts_until_failure(&ctx, close_account).await
}

/// A failure whose type the policy does not list is retried as usual.
#[workflow(name = "task_unlisted_retried")]
pub async fn task_unlisted_retried(ctx: WorkflowContext, _input: Empty) -> Result<CatchResult> {
    attempts_until_failure(&ctx, throttle).await
}

/// Runs a task that fails, catches the durable `TaskFailed` error, and reports what
/// it observed. Guards the durable task-failure decode contract.
#[workflow(name = "catch_task_failure")]
pub async fn catch_task_failure(ctx: WorkflowContext, _input: Empty) -> Result<CatchResult> {
    let outcome: Result<Empty> = ctx.execute_task(always_fail, Empty::default()).await;
    match outcome {
        // The task unexpectedly succeeded. Return a result the harness can see
        // (it expects caught=true) rather than failing the workflow.
        Ok(_) => Ok(CatchResult {
            caught: false,
            task_type: String::new(),
            attempts: 0,
        }),
        Err(Error::Workflow(WorkflowError::TaskFailed {
            task_type,
            attempts,
            ..
        })) => Ok(CatchResult {
            caught: true,
            task_type,
            attempts,
        }),
        // Any other error is unexpected: propagate it so the workflow fails and
        // the harness marks the scenario failed.
        Err(other) => Err(other),
    }
}

#[derive(Serialize, Deserialize)]
pub struct ValueInput {
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct EchoedOutput {
    pub echoed: String,
}

/// Returns its input value under `echoed`. Exercises task-input round-tripping.
#[task]
pub async fn echo_task(_ctx: TaskContext, input: ValueInput) -> Result<EchoedOutput> {
    Ok(EchoedOutput {
        echoed: input.value,
    })
}

/// Passes a value through a task and back. Guards that a task receives its
/// whole input intact.
#[workflow(name = "task_input_roundtrip")]
pub async fn task_input_roundtrip(ctx: WorkflowContext, input: ValueInput) -> Result<EchoedOutput> {
    let out: EchoedOutput = ctx
        .execute_task(echo_task, ValueInput { value: input.value })
        .await?;
    Ok(out)
}

#[derive(Serialize, Deserialize)]
pub struct ChildInput {
    pub n: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ChildOutput {
    pub child_saw: i64,
    pub doubled: i64,
}

/// A child workflow: echoes what it received and doubles it. Proves a child
/// runs its own logic and returns a result to its parent.
#[workflow(name = "contract_child")]
pub async fn contract_child(_ctx: WorkflowContext, input: ChildInput) -> Result<ChildOutput> {
    Ok(ChildOutput {
        child_saw: input.n,
        doubled: input.n * 2,
    })
}

/// Starts a child workflow, waits for its result, and returns it. Guards the
/// full child-workflow round trip: StartChildWorkflow emission, child
/// execution, and the completion decode that resumes the parent. The child
/// must run exactly once, not again on every replay of the parent.
#[workflow(name = "parent_starts_child")]
pub async fn parent_starts_child(ctx: WorkflowContext, input: ChildInput) -> Result<ChildOutput> {
    let out: ChildOutput = ctx.execute_child_workflow("contract_child", input).await?;
    Ok(out)
}

#[derive(Serialize, Deserialize, Default)]
pub struct RestartInput {
    #[serde(default)]
    pub count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct RestartResult {
    pub restarted: bool,
    pub count: i64,
}

/// Restarts fresh once, then completes. Guards restart-fresh persistence and
/// that the client following the original execution receives the result of
/// the restarted one.
#[workflow(name = "restart_once")]
pub async fn restart_once(ctx: WorkflowContext, input: RestartInput) -> Result<RestartResult> {
    if input.count == 0 {
        ctx.restart_fresh_with(RestartInput { count: 1 }, None)?;
        // Never observed: the restart is the terminal action, and the SDK
        // discards any return value once a RestartFresh command is emitted.
        return Ok(RestartResult {
            restarted: true,
            count: 0,
        });
    }
    Ok(RestartResult {
        restarted: true,
        count: input.count,
    })
}

#[derive(Serialize, Deserialize)]
pub struct ChildFailResult {
    pub child_failed: bool,
}

/// A child workflow that always fails. Exercises child-failure propagation
/// (engine ChildWorkflowExecutionFailed) and the failure decode.
#[workflow(name = "failing_child")]
pub async fn failing_child(_ctx: WorkflowContext, _input: Empty) -> Result<Empty> {
    Err(Error::Workflow(WorkflowError::Panic(
        "intentional child failure".to_string(),
    )))
}

/// Starts a failing child, catches the durable child failure, and reports it.
/// Guards that a child failure propagates to and is catchable by the parent.
#[workflow(name = "parent_catches_child_failure")]
pub async fn parent_catches_child_failure(
    ctx: WorkflowContext,
    _input: Empty,
) -> Result<ChildFailResult> {
    let outcome: Result<Empty> = ctx
        .execute_child_workflow("failing_child", Empty::default())
        .await;
    match outcome {
        // The child unexpectedly succeeded. Return a result the harness can see
        // (it expects child_failed=true).
        Ok(_) => Ok(ChildFailResult {
            child_failed: false,
        }),
        Err(Error::Workflow(WorkflowError::ChildWorkflowFailed { .. })) => {
            Ok(ChildFailResult { child_failed: true })
        }
        // Any other error is unexpected: propagate it so the workflow fails.
        Err(other) => Err(other),
    }
}

#[derive(Serialize, Deserialize)]
pub struct TwoChildrenResult {
    pub first: i64,
    pub second: i64,
}

/// Starts two child workflows in parallel, then awaits both handles. Both
/// StartChildWorkflow commands are emitted before either result is awaited,
/// because `start_child_workflow` does not suspend. Guards the handle path
/// (`start_child_workflow` + `handle.result()`) and the engine's idempotent
/// child start: when the parent replays after one child completes, re-emitting
/// the still-pending child's start must return the existing child, not spawn a
/// duplicate.
#[workflow(name = "parent_two_children_parallel")]
pub async fn parent_two_children_parallel(
    ctx: WorkflowContext,
    _input: Empty,
) -> Result<TwoChildrenResult> {
    let a = ctx
        .start_child_workflow("contract_child", ChildInput { n: 10 })
        .await?;
    let b = ctx
        .start_child_workflow("contract_child", ChildInput { n: 20 })
        .await?;
    let first: ChildOutput = a.result().await?;
    let second: ChildOutput = b.result().await?;
    Ok(TwoChildrenResult {
        first: first.doubled,
        second: second.doubled,
    })
}

#[derive(Serialize, Deserialize)]
pub struct SleptResult {
    pub slept: bool,
}

/// Sleeps on a durable timer, then completes. Guards the durable-timer round
/// trip: StartTimer emission, the engine firing the timer, and the FireTimer
/// correlation that resumes the workflow by the business timer id on replay.
#[workflow(name = "sleep_once")]
pub async fn sleep_once(ctx: WorkflowContext, _input: Empty) -> Result<SleptResult> {
    ctx.sleep(std::time::Duration::from_secs(1)).await?;
    Ok(SleptResult { slept: true })
}

// --- Event catalog -----------------------------------------------------------
//
// These cover the wait-for-event path end to end: the activation that parks a
// workflow must be accepted, a client must be able to address the parked
// workflow to wake it, and the engine must not dispatch the parked workflow
// again and again while it waits.
//
// Observation is in-process, like the actor gauges: the driver runs the worker,
// so the catalog can tell it when a workflow has actually parked and how many
// times the engine activated it. Both are keyed by the `key` the scenario
// passes as input, because the engine hands the worker its own internal id as
// the workflow id, which the driver never sees.

/// Keys of workflows that have issued their wait and are about to suspend.
pub static PARKED_WORKFLOWS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<String>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

/// Activations per key: every time the engine ran the workflow function,
/// replays included. A parked workflow should be activated a handful of times
/// (park, wake, complete), not hundreds.
pub static ACTIVATIONS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, u32>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

fn record_activation(key: &str) -> u32 {
    let mut activations = ACTIVATIONS.lock().unwrap();
    let n = activations.entry(key.to_string()).or_insert(0);
    *n += 1;
    *n
}

fn mark_parked(key: &str) {
    PARKED_WORKFLOWS.lock().unwrap().insert(key.to_string());
}

#[derive(Serialize, Deserialize)]
pub struct KeyedInput {
    pub key: String,
}

#[derive(Serialize, Deserialize)]
pub struct EventEchoResult {
    pub received: serde_json::Value,
    pub activations: u32,
}

/// Parks on `contract_event`, then returns the payload it received together
/// with how many times it was activated to get there.
///
/// Guards the whole event round trip: the parking activation is accepted by
/// the engine, the parked workflow stays parked (one activation, not a storm),
/// a client can wake it by workflow id alone, and the replay consumes the
/// journaled event and completes.
#[workflow(name = "wait_for_event_echo")]
pub async fn wait_for_event_echo(
    ctx: WorkflowContext,
    input: KeyedInput,
) -> Result<EventEchoResult> {
    let count = record_activation(&input.key);
    mark_parked(&input.key);
    let received: serde_json::Value = ctx.wait_for_event("contract_event").await?;
    Ok(EventEchoResult {
        received,
        activations: count,
    })
}

#[derive(Serialize, Deserialize)]
pub struct EventTimeoutResult {
    pub received: Option<serde_json::Value>,
    pub timed_out: bool,
}

/// Parks on `contract_event` with a two-second timeout that nobody satisfies,
/// and reports whether the timeout fired.
///
/// No SDK evaluates this timeout and the engine never receives it, so the
/// workflow waits forever. The scenario is marked as a known gap and passes
/// once the timeout is implemented.
#[workflow(name = "wait_for_event_timeout")]
pub async fn wait_for_event_timeout(
    ctx: WorkflowContext,
    input: KeyedInput,
) -> Result<EventTimeoutResult> {
    record_activation(&input.key);
    mark_parked(&input.key);
    let received: Option<serde_json::Value> = ctx
        .wait_for_event_with_timeout("contract_event", std::time::Duration::from_secs(2))
        .await?;
    Ok(EventTimeoutResult {
        timed_out: received.is_none(),
        received,
    })
}

// --- Actor catalog -----------------------------------------------------------
//
// `contract_probe` checks the shared/exclusive scheduling contract end to end:
// declared mode, handler registration, engine resolution, dispatch.
// Concurrency is observed through in-process gauges (statics): the engine
// dispatches both operations to this same worker process, so a gauge reaching
// 2 proves real overlap. The gauges cannot live in durable actor state because
// shared operations must stay read-only (the engine rejects their writes, and
// that rejection is itself a scenario).

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

static EXCLUSIVE_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);

/// Monotonic count of rendezvous entries per actor key. Monotonic (rather
/// than an in-flight gauge) so the first entrant still observes the second
/// even if the second checks, returns, and exits between the first's polls.
static RENDEZVOUS_ENTERED: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, u32>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

const RENDEZVOUS_DEADLINE_MS: u64 = 5_000;
const RENDEZVOUS_POLL_MS: u64 = 25;
const EXCLUSIVE_HOLD_MS: u64 = 250;

#[derive(Serialize, Deserialize)]
pub struct RendezvousResult {
    pub observed_concurrent: bool,
}

#[derive(Serialize, Deserialize)]
pub struct AloneResult {
    pub alone: bool,
}

#[derive(Serialize, Deserialize)]
pub struct WriteAttemptResult {
    pub write_rejected: bool,
    pub error: String,
}

#[derive(Serialize, Deserialize)]
pub struct WriteRoundtripResult {
    pub write_ok: bool,
    pub value: String,
}

/// Probe actor observing the scheduler's shared/exclusive behavior.
#[actor(name = "contract_probe")]
pub struct ContractProbe;

#[operations]
impl ContractProbe {
    /// Wait until a second shared invocation has entered on this worker.
    ///
    /// If shared operations dispatch concurrently (correct), the second
    /// invocation enters while the first is still waiting, so both observe an
    /// entry count of 2 and return `observed_concurrent: true`. If the
    /// scheduler wrongly serializes them, the first invocation exhausts the
    /// deadline before the second can enter and reports false.
    #[operation(shared)]
    pub async fn shared_rendezvous(ctx: ActorContext) -> Result<RendezvousResult> {
        let key = ctx.key().to_string();
        {
            let mut entered = RENDEZVOUS_ENTERED.lock().unwrap();
            *entered.entry(key.clone()).or_insert(0) += 1;
        }

        let mut waited = 0u64;
        let mut observed;
        loop {
            observed = RENDEZVOUS_ENTERED
                .lock()
                .unwrap()
                .get(&key)
                .copied()
                .unwrap_or(0)
                >= 2;
            if observed || waited >= RENDEZVOUS_DEADLINE_MS {
                break;
            }
            tokio::time::sleep(Duration::from_millis(RENDEZVOUS_POLL_MS)).await;
            waited += RENDEZVOUS_POLL_MS;
        }
        Ok(RendezvousResult {
            observed_concurrent: observed,
        })
    }

    /// Hold the key briefly and report whether any peer overlapped.
    #[operation(exclusive)]
    pub async fn exclusive_probe(_ctx: ActorContext) -> Result<AloneResult> {
        EXCLUSIVE_IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        let mut alone = EXCLUSIVE_IN_FLIGHT.load(Ordering::SeqCst) == 1;
        tokio::time::sleep(Duration::from_millis(EXCLUSIVE_HOLD_MS)).await;
        alone = alone && EXCLUSIVE_IN_FLIGHT.load(Ordering::SeqCst) == 1;
        EXCLUSIVE_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        Ok(AloneResult { alone })
    }

    /// Attempt a state write from a read-only operation; expect rejection.
    #[operation(shared)]
    pub async fn shared_write_attempt(ctx: ActorContext) -> Result<WriteAttemptResult> {
        match ctx.state().set("probe", &"should-be-rejected").await {
            Ok(()) => Ok(WriteAttemptResult {
                write_rejected: false,
                error: String::new(),
            }),
            Err(e) => Ok(WriteAttemptResult {
                write_rejected: true,
                error: e.to_string().chars().take(200).collect(),
            }),
        }
    }

    /// Control: exclusive writes must succeed and round-trip.
    #[operation(exclusive)]
    pub async fn exclusive_write_roundtrip(
        ctx: ActorContext,
        input: ValueInput,
    ) -> Result<WriteRoundtripResult> {
        ctx.state().set("value", &input.value).await?;
        let read: Option<String> = ctx.state().get("value").await?;
        let read = read.unwrap_or_default();
        Ok(WriteRoundtripResult {
            write_ok: read == input.value,
            value: read,
        })
    }
}

/// Reports back the heartbeat timeout this task was actually given.
///
/// Guards the whole declaration path: declared by the workflow, recorded by
/// the engine, delivered on poll, and readable by the handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedTimeouts {
    pub heartbeat_secs: Option<u64>,
}

#[task]
pub async fn timeouts_echo_task(ctx: TaskContext, _input: Empty) -> Result<ObservedTimeouts> {
    Ok(ObservedTimeouts {
        heartbeat_secs: ctx.heartbeat_timeout().map(|d| d.as_secs()),
    })
}

// The scenario is about the per-call timeouts in `TaskOptions`, and this is the
// call that takes them.
#[allow(deprecated)]
#[workflow]
pub async fn task_timeouts_echo(ctx: WorkflowContext, _input: Empty) -> Result<ObservedTimeouts> {
    ctx.execute_task_with_options(
        "timeouts_echo_task",
        Empty::default(),
        orcher::workflow::TaskOptions::default()
            .with_heartbeat_timeout(std::time::Duration::from_secs(15)),
    )
    .await
}

/// Returns the SDK's typed workflow error.
///
/// The assertion is that the message survives the trip. A failure the engine
/// cannot parse still counts as "the workflow failed", so checking the status
/// alone would miss an error that cannot be reported.
#[workflow]
pub async fn typed_failure(_ctx: WorkflowContext, _input: Empty) -> Result<Empty> {
    Err(Error::Workflow(WorkflowError::StateError(
        "deliberate contract failure".to_string(),
    )))
}

#[derive(Serialize, Deserialize)]
pub struct SleepInput {
    pub sleep_secs: u64,
}

/// What a task that outlived its heartbeat timeout saw.
#[derive(Serialize, Deserialize)]
pub struct OutlivedHeartbeat {
    pub runs: u32,
    pub heartbeat_timeout_given: bool,
    pub outlived_heartbeat_timeout: bool,
}

/// How many times this worker process has started the task, by workflow id:
/// a task timed out and retried here runs twice.
static RUNS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, u32>>> =
    std::sync::LazyLock::new(Default::default);

/// Sleeps `sleep_secs` with no heartbeat code of its own, and says how many
/// times it has run for its workflow and whether the sleep outlasted the
/// heartbeat timeout it was held to. Only the worker's own heartbeats keep it
/// alive meanwhile.
#[task]
pub async fn sleep_past_heartbeat_timeout(
    ctx: TaskContext,
    input: SleepInput,
) -> Result<OutlivedHeartbeat> {
    let runs = {
        let mut runs = RUNS.lock().unwrap_or_else(|p| p.into_inner());
        let n = runs.entry(ctx.workflow_id().to_string()).or_default();
        *n += 1;
        *n
    };
    tokio::time::sleep(std::time::Duration::from_secs(input.sleep_secs)).await;
    let timeout = ctx.heartbeat_timeout();
    Ok(OutlivedHeartbeat {
        runs,
        heartbeat_timeout_given: timeout.is_some(),
        outlived_heartbeat_timeout: timeout
            .is_some_and(|t| std::time::Duration::from_secs(input.sleep_secs) > t),
    })
}

/// Runs a task that declares no timeouts for longer than its heartbeat
/// timeout.
#[workflow]
pub async fn outlive_heartbeat_timeout(
    ctx: WorkflowContext,
    input: SleepInput,
) -> Result<OutlivedHeartbeat> {
    ctx.execute_task(sleep_past_heartbeat_timeout, input).await
}

/// What the task that outlived its worker saw.
#[derive(Serialize, Deserialize)]
pub struct SurvivedWorkerDeath {
    pub survived_worker_death: bool,
}

/// Takes its worker process down when run by a worker started with
/// `ORCHER_CONTRACT_DOOMED_WORKER` set, as a worker dying mid-task would;
/// completes on any other.
#[task(retry = 3)]
pub async fn exit_if_worker_doomed(
    _ctx: TaskContext,
    _input: Empty,
) -> Result<SurvivedWorkerDeath> {
    if std::env::var_os("ORCHER_CONTRACT_DOOMED_WORKER").is_some() {
        eprintln!("contract: exit_if_worker_doomed is taking its worker down");
        std::process::exit(3);
    }
    Ok(SurvivedWorkerDeath {
        survived_worker_death: true,
    })
}

/// Runs a task that takes a doomed worker down with it; completes once the
/// task has run to the end on another.
#[workflow]
pub async fn crash_mid_task(ctx: WorkflowContext, _input: Empty) -> Result<SurvivedWorkerDeath> {
    ctx.execute_task(exit_if_worker_doomed, Empty::default())
        .await
}
