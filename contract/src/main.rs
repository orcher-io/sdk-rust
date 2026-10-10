//! Contract suite driver: a Rust worker plus the scenarios it is checked against.
//!
//! Starts a worker that serves the shared contract catalog, then runs every
//! scenario in `scenarios.json` against a live engine and checks that the
//! terminal status and returned result match `expect`. Every assertion is
//! client-observable (terminal state and returned JSON), so the same
//! `scenarios.json` drives the Python and TypeScript workers unchanged.
//!
//! Usage, against an engine already listening on :50051:
//!   cargo run -p orcher-contract -- --server-url http://localhost:50051
//!
//! Exits non-zero if any scenario fails.

mod catalog;

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions, WorkflowIdReusePolicy};
use orcher_sdk::prelude::*;
use serde::Deserialize;
use serde_json::Value;

#[derive(Parser, Debug, Clone)]
#[command(about = "ORCHER cross-SDK contract harness (Rust worker + driver)")]
struct Args {
    /// Engine gRPC endpoint.
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,

    /// Namespace to run in.
    ///
    /// A real namespace, not `default`. A child workflow created in `default`
    /// instead of its parent's namespace is stranded where no worker claims
    /// it, but in `default` itself the wrong namespace and the right one are
    /// the same string, so the child-workflow scenarios could not tell them
    /// apart.
    ///
    /// Created on start when it does not exist yet.
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "contract")]
    namespace: String,

    /// Task queue the worker serves and scenarios start on.
    #[arg(long, default_value = "contract")]
    task_queue: String,

    /// Path to the scenario spec.
    #[arg(long, default_value = "contract/scenarios.json")]
    scenarios: String,

    /// Seconds to wait for a workflow to reach a terminal state.
    #[arg(long, default_value_t = 30)]
    result_timeout_secs: u64,

    /// Run only a worker on `--task-queue`, and no scenarios. The harness
    /// starts itself this way for `kind: "worker_crash"`, whose task takes its
    /// worker process down.
    #[arg(long)]
    worker_only: bool,
}

#[derive(Deserialize)]
struct Spec {
    scenarios: Vec<Scenario>,
}

#[derive(Deserialize, Clone)]
struct Scenario {
    id: String,
    /// Scenario kind: absent/"workflow" starts a workflow; "actor" invokes
    /// operations on a catalog actor; "reset" runs a workflow to completion,
    /// resets it to `reset_to_event_id`, and asserts the successor run;
    /// "client_error" makes a failing client call and asserts the error code;
    /// "workflow_id_reuse" starts one workflow id again while it runs and
    /// after it completes, and asserts which starts are refused; "cancel"
    /// cancels the workflow once it has parked and asserts how it ended.
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    workflow: Option<String>,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    expect: Option<Expect>,
    #[serde(default)]
    actor: Option<String>,
    #[serde(default)]
    steps: Vec<Step>,
    /// Journal event to reset to (inclusive), for `kind: "reset"`.
    #[serde(default)]
    reset_to_event_id: Option<i64>,
    /// Named client call to make, for `kind: "client_error"`.
    #[serde(default)]
    client_call: Option<String>,
    /// Event to send once the workflow has parked, for `kind: "event"`.
    #[serde(default)]
    event_name: Option<String>,
    #[serde(default)]
    event_payload: Value,
    /// Set when the behavior is specified but not implemented. The scenario is
    /// expected to fail; it is reported as such and does not fail the run, and
    /// an unexpected pass is reported so the flag can be removed.
    #[serde(default)]
    known_gap: Option<String>,
    /// SDKs the gap applies to; absent means all of them. Lets one SDK close
    /// a gap while the shared file keeps it open for the others.
    #[serde(default)]
    known_gap_sdks: Option<Vec<String>>,
}

/// This runner's name in `known_gap_sdks`.
const THIS_SDK: &str = "rust";

impl Scenario {
    /// The gap description, if the gap applies to this SDK.
    fn applicable_gap(&self) -> Option<&String> {
        let applies = self
            .known_gap_sdks
            .as_ref()
            .is_none_or(|sdks| sdks.iter().any(|s| s == THIS_SDK));
        self.known_gap.as_ref().filter(|_| applies)
    }
}

#[derive(Deserialize, Clone)]
struct Expect {
    /// "completed" or "failed". Absent for `kind: "client_error"`, which
    /// asserts on `error_code` instead of a terminal workflow state.
    #[serde(default)]
    status: String,
    /// Expected returned result for a completed workflow (ignored for "failed").
    #[serde(default)]
    result: Value,
    /// Expected error code, for `kind: "client_error"`.
    #[serde(default)]
    error_code: Option<String>,
    /// Substring the surfaced failure must contain, for `status: "failed"`.
    ///
    /// Without it any failure passes, including one the engine could not
    /// parse, so an error that cannot be reported would go unnoticed.
    #[serde(default)]
    error_contains: Option<String>,
    /// Wall-clock bounds on the whole run, for scenarios whose duration is part
    /// of the contract: a durable timer must actually wait, and must not wait
    /// far longer than it was asked to.
    #[serde(default)]
    min_elapsed_ms: Option<u128>,
    #[serde(default)]
    max_elapsed_ms: Option<u128>,
    /// Upper bounds on numeric result fields, for behavior that is a count
    /// rather than a value: a parked workflow is activated a few times, and
    /// "a few" is the contract, not an exact number.
    #[serde(default)]
    result_at_most: std::collections::HashMap<String, f64>,
}

/// One step of an actor scenario: `parallel` concurrent invocations of
/// `operation`, each asserted against `expect_each`.
#[derive(Deserialize, Clone)]
struct Step {
    operation: String,
    #[serde(default)]
    input: Value,
    #[serde(default = "default_parallel")]
    parallel: usize,
    #[serde(default)]
    expect_each: Value,
}

fn default_parallel() -> usize {
    1
}

/// Create `name` unless it already exists.
async fn ensure_namespace(server_url: &str, name: &str) -> Result<()> {
    let client = Client::with_config(ClientConfig::new(server_url.to_string()))
        .await
        .context("connecting client to create the namespace")?;
    match client.create_namespace(name, 7).await {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("already exists") => Ok(()),
        Err(e) => Err(e).with_context(|| format!("creating namespace '{name}'")),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let args = Args::parse();

    // Before the worker starts polling it, and with nothing outside the
    // harness: the suite used to need the CLI just for this.
    ensure_namespace(&args.server_url, &args.namespace).await?;

    if args.worker_only {
        let worker = Worker::builder()
            .server_url(&args.server_url)
            .task_queue(&args.task_queue)
            .namespace(&args.namespace)
            .build()
            .await?;
        return Ok(worker.run().await?);
    }

    let spec: Spec = {
        let raw = std::fs::read_to_string(&args.scenarios)
            .with_context(|| format!("reading scenarios from {}", args.scenarios))?;
        serde_json::from_str(&raw).context("parsing scenarios.json")?
    };

    println!(
        "ORCHER contract — Rust worker | {} scenarios | server {}",
        spec.scenarios.len(),
        args.server_url
    );

    // Start the worker in the background. The catalog registers itself through
    // the macros' auto-registration.
    let worker_args = args.clone();
    let worker_handle = tokio::spawn(async move {
        let worker = Worker::builder()
            .server_url(&worker_args.server_url)
            .task_queue(&worker_args.task_queue)
            .namespace(&worker_args.namespace)
            .build()
            .await?;
        worker.run().await
    });

    // Give the worker a moment to register and start polling.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = Client::with_config(
        ClientConfig::new(args.server_url.clone())
            .with_namespace(args.namespace.clone())
            .with_timeout(Duration::from_secs(args.result_timeout_secs + 10)),
    )
    .await
    .context("connecting client")?;

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for scenario in &spec.scenarios {
        let is_actor = scenario.kind.as_deref() == Some("actor");
        let label = if is_actor {
            scenario.actor.clone().unwrap_or_default()
        } else {
            scenario
                .workflow
                .clone()
                .or_else(|| scenario.client_call.clone())
                .unwrap_or_default()
        };
        let outcome = match scenario.kind.as_deref() {
            Some("actor") => run_actor_scenario(&client, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("reset") => run_reset_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("client_error") => run_client_error_scenario(&client, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("event") => run_event_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("cancel") => run_cancel_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("workflow_id_reuse") => run_workflow_id_reuse_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
            Some("engine_restart") => run_engine_restart_scenario(&client, &args, scenario).await,
            Some("worker_crash") => run_worker_crash_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
            _ => run_scenario(&client, &args, scenario)
                .await
                .map(|()| Outcome::Ran),
        };
        match (scenario.applicable_gap(), outcome) {
            // A scenario that did not run asserts nothing, so it is neither a
            // pass nor a failure. Counting it as a pass would report the
            // engine-restart check as green whenever its restart command is
            // not configured.
            (_, Ok(Outcome::Skipped(why))) => {
                println!("  SKIP  {} ({label}): {why}", scenario.id);
                skipped += 1;
            }
            // Specified but not implemented: a failure is expected and does not
            // fail the run; a pass means the gap is closed and the flag should go.
            (Some(gap), Ok(Outcome::Ran)) => {
                println!(
                    "  FAIL  {} ({label}): passed but is marked known_gap — remove the flag: {gap}",
                    scenario.id
                );
                failed += 1;
            }
            (Some(gap), Err(_)) => {
                println!("  XFAIL {} ({label}): known gap — {gap}", scenario.id);
                passed += 1;
            }
            (None, Ok(Outcome::Ran)) => {
                println!("  PASS  {} ({label})", scenario.id);
                passed += 1;
            }
            (None, Err(e)) => {
                println!("  FAIL  {} ({label}): {e:#}", scenario.id);
                failed += 1;
            }
        }
    }

    worker_handle.abort();

    println!(
        "\n{passed} passed, {failed} failed, {skipped} skipped, {} total",
        spec.scenarios.len()
    );
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Make the named failing client call.
///
/// Each call is a plain client operation whose failure every SDK is expected to
/// report identically. Only operations that need no worker are used, so the
/// assertion is about the client's error contract and nothing else.
async fn perform_client_call(client: &Client, call: &str) -> Result<(), orcher_sdk::Error> {
    match call {
        "status_of_missing_workflow" => {
            // A fresh id per run, as in the other scenarios.
            let workflow_id = format!(
                "contract-missing-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let handle = client.get_workflow_handle(&workflow_id).await?;
            handle.status().await?;
            Ok(())
        }
        other => panic!("unknown client_call: {other}"),
    }
}

/// Run one client-error scenario: make the call, require it to fail, and assert
/// the reported code.
///
/// The assertion is on the code rather than the type name. The SDKs
/// legitimately differ in the type (this one reports a client-error variant
/// where the others report a workflow error), but the code is the contract a
/// user relies on across languages, so every SDK must report the same code for
/// the same condition.
async fn run_client_error_scenario(client: &Client, scenario: &Scenario) -> Result<()> {
    let expected = scenario
        .expect
        .as_ref()
        .and_then(|e| e.error_code.clone())
        .context("client_error scenario missing `expect.error_code`")?;
    let call = scenario
        .client_call
        .as_deref()
        .context("client_error scenario missing `client_call`")?;

    match perform_client_call(client, call).await {
        Ok(()) => anyhow::bail!("expected the call to fail with {expected}, but it succeeded"),
        Err(e) => {
            let actual = e.code().to_string();
            if actual != expected {
                anyhow::bail!("expected error code {expected}, got {actual} ({e})");
            }
            Ok(())
        }
    }
}

/// Start one scenario's workflow, wait for a terminal state, and assert `expect`.
async fn run_scenario(client: &Client, args: &Args, scenario: &Scenario) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("workflow scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("workflow scenario missing `expect`"))?;

    let options =
        StartWorkflowOptions::new(args.task_queue.clone()).with_namespace(args.namespace.clone());

    let started_at = std::time::Instant::now();
    let handle = client
        .start_workflow_with_options(workflow, scenario.input.clone(), options)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;

    let outcome = handle
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
        .await;

    match (expect.status.as_str(), outcome) {
        ("completed", Ok(result)) => {
            // Duration is part of the contract for timer scenarios: asserting
            // only the result would accept a 1s timer that takes 30s.
            let elapsed_ms = started_at.elapsed().as_millis();
            if let Some(min) = expect.min_elapsed_ms {
                if elapsed_ms < min {
                    return Err(anyhow!(
                        "completed too fast: {elapsed_ms}ms < {min}ms (the wait did not happen)"
                    ));
                }
            }
            if let Some(max) = expect.max_elapsed_ms {
                if elapsed_ms > max {
                    return Err(anyhow!(
                        "took too long: {elapsed_ms}ms > {max}ms (waited far longer than asked)"
                    ));
                }
            }

            if json_matches(&expect.result, &result) {
                Ok(())
            } else {
                Err(anyhow!(
                    "result mismatch\n    expected: {}\n    actual:   {}",
                    expect.result,
                    result
                ))
            }
        }
        ("completed", Err(e)) => Err(anyhow!("expected completion but workflow failed: {e}")),
        ("failed", Err(e)) => {
            if let Some(wanted) = expect.error_contains.as_deref() {
                let surfaced = e.to_string();
                if !surfaced.contains(wanted) {
                    return Err(anyhow!(
                        "failed as expected, but the message did not survive\n    wanted substring: {wanted}\n    surfaced:         {surfaced}"
                    ));
                }
            }
            Ok(())
        }
        ("failed", Ok(result)) => Err(anyhow!(
            "expected failure but workflow completed with: {result}"
        )),
        (other, _) => Err(anyhow!("unknown expected status '{other}'")),
    }
}

/// Run one reset scenario: run a workflow to completion, reset it to
/// `reset_to_event_id`, then assert the successor run replays the copied prefix
/// and reaches `expect`.
///
/// This exercises the whole reset path, not just the client binding: the engine
/// reads the journal by the execution row's internal id (not the user-facing
/// workflow id), must seed the successor's journal before the run becomes
/// dispatchable, and must be able to persist the `reset` status on the original.
async fn run_reset_scenario(client: &Client, args: &Args, scenario: &Scenario) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("reset scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("reset scenario missing `expect`"))?;
    let target_event_id = scenario
        .reset_to_event_id
        .ok_or_else(|| anyhow!("reset scenario missing `reset_to_event_id`"))?;

    // A fresh id per run, as the actor scenarios use fresh keys.
    let workflow_id = format!(
        "conf-reset-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let options = StartWorkflowOptions::new(args.task_queue.clone())
        .with_workflow_id(workflow_id.clone())
        .with_namespace(args.namespace.clone());

    let handle = client
        .start_workflow_with_options(workflow, scenario.input.clone(), options)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;

    handle
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
        .await
        .context("original run did not complete")?;

    let new_execution_id = handle
        .reset(target_event_id, "contract reset")
        .await
        .context("reset rejected")?;

    if new_execution_id.is_empty() {
        return Err(anyhow!(
            "reset returned an empty execution id (reported success without resetting)"
        ));
    }

    let successor = client
        .get_workflow_handle_with_run_id(&workflow_id, Some(new_execution_id.clone()))
        .await
        .context("getting successor handle")?;

    let result = successor
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
        .await
        .with_context(|| format!("successor run {new_execution_id} did not complete"))?;

    if expect.status != "completed" {
        return Err(anyhow!(
            "reset scenarios only support an expected status of 'completed', got '{}'",
            expect.status
        ));
    }
    if json_matches(&expect.result, &result) {
        Ok(())
    } else {
        Err(anyhow!(
            "successor result mismatch\n    expected: {}\n    actual:   {}",
            expect.result,
            result
        ))
    }
}

/// Run one actor scenario: for each step, fire `parallel` concurrent
/// invocations against a fresh per-run key and assert every invocation's
/// result against `expect_each`.
async fn run_actor_scenario(client: &Client, scenario: &Scenario) -> Result<()> {
    let actor = scenario
        .actor
        .as_deref()
        .ok_or_else(|| anyhow!("actor scenario missing `actor`"))?;

    // Fresh key per run so durable state from prior runs can't interfere.
    let key = format!(
        "conf-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    for step in &scenario.steps {
        let invocations = (0..step.parallel).map(|_| {
            client.invoke_actor::<Value, Value>(actor, &key, &step.operation, step.input.clone())
        });
        let results = futures::future::join_all(invocations).await;

        for outcome in results {
            let result =
                outcome.with_context(|| format!("invoking operation {}", step.operation))?;
            if !json_matches(&step.expect_each, &result) {
                return Err(anyhow!(
                    "result mismatch on {}\n    expected: {}\n    actual:   {}",
                    step.operation,
                    step.expect_each,
                    result
                ));
            }
        }
    }
    Ok(())
}

/// Wait until the catalog reports the keyed workflow parked, or give up. The
/// worker runs in this process, so "parked" is observed directly rather than
/// inferred from a status the engine reports as RUNNING either way.
async fn wait_until_parked(key: &str, deadline: Duration) -> bool {
    let until = std::time::Instant::now() + deadline;
    while std::time::Instant::now() < until {
        if catalog::PARKED_WORKFLOWS.lock().unwrap().contains(key) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Numeric upper bounds: every listed field must be present in `actual` and
/// no greater than the bound.
fn within_bounds(bounds: &std::collections::HashMap<String, f64>, actual: &Value) -> Result<()> {
    let object = actual
        .as_object()
        .ok_or_else(|| anyhow!("result is not an object"))?;
    for (field, max) in bounds {
        let value = object
            .get(field)
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow!("{field} is missing or not a number"))?;
        if value > *max {
            return Err(anyhow!("{field} = {value}, expected at most {max}"));
        }
    }
    Ok(())
}

fn fresh_key(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Run one cancellation scenario: start the workflow, cancel it once it has
/// parked, and assert how it ended.
///
/// `expect.status` is `cancelled` for a workflow that lets the cancellation it
/// is told of end it, and `completed`, with `expect.result`, for one that
/// cleans up and returns.
async fn run_cancel_scenario(client: &Client, args: &Args, scenario: &Scenario) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("cancel scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("cancel scenario missing `expect`"))?;

    let key = fresh_key("conf");
    let mut input = scenario.input.clone();
    if !input.is_object() {
        input = serde_json::json!({});
    }
    input["key"] = Value::String(key.clone());

    let options =
        StartWorkflowOptions::new(args.task_queue.clone()).with_namespace(args.namespace.clone());
    let handle = client
        .start_workflow_with_options(workflow, input, options)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;

    let deadline = Duration::from_secs(args.result_timeout_secs);
    if !wait_until_parked(&key, deadline).await {
        let _ = handle.cancel().await;
        return Err(anyhow!("the workflow never parked"));
    }
    // As for an event: let the engine record the parking activation, so the
    // cancellation wakes a parked workflow rather than racing its first run.
    tokio::time::sleep(Duration::from_millis(500)).await;
    handle.cancel().await.context("cancelling the workflow")?;

    match expect.status.as_str() {
        "cancelled" => {
            let until = std::time::Instant::now() + deadline;
            loop {
                match handle.status().await.context("reading the status")? {
                    WorkflowStatus::Cancelled => return Ok(()),
                    WorkflowStatus::Running if std::time::Instant::now() < until => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    other => {
                        return Err(anyhow!("expected the workflow cancelled, it is {other:?}"))
                    }
                }
            }
        }
        "completed" => {
            let result = handle
                .result_with_timeout::<Value>(deadline)
                .await
                .map_err(|e| anyhow!("expected completion after cleanup but got: {e}"))?;
            if json_matches(&expect.result, &result) {
                Ok(())
            } else {
                Err(anyhow!(
                    "result mismatch\n    expected: {}\n    actual:   {}",
                    expect.result,
                    result
                ))
            }
        }
        other => Err(anyhow!(
            "unknown expected status '{other}' for a cancel scenario"
        )),
    }
}

/// Run one event scenario: start the workflow, wait for it to park, wake it
/// with the named event by workflow id alone, and assert the result.
///
/// This checks that the parking activation is accepted, that the wake can be
/// addressed without a run id, and that a parked workflow is not dispatched
/// again and again while it waits. The activation count in the result, bounded
/// by `expect.result_at_most`, is what catches the last.
async fn run_event_scenario(client: &Client, args: &Args, scenario: &Scenario) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("event scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("event scenario missing `expect`"))?;
    let event_name = scenario
        .event_name
        .as_deref()
        .ok_or_else(|| anyhow!("event scenario missing `event_name`"))?;

    let key = fresh_key("conf");
    let workflow_id = format!("conf-event-{key}");
    let mut input = scenario.input.clone();
    if !input.is_object() {
        input = serde_json::json!({});
    }
    input["key"] = Value::String(key.clone());

    let options = StartWorkflowOptions::new(args.task_queue.clone())
        .with_workflow_id(workflow_id.clone())
        .with_namespace(args.namespace.clone());
    let handle = client
        .start_workflow_with_options(workflow, input, options)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;

    if !wait_until_parked(&key, Duration::from_secs(args.result_timeout_secs)).await {
        return Err(anyhow!(
            "the workflow never parked (the wait was not reached, or the activation failed)"
        ));
    }
    // A parked workflow holds its claim; give the engine a moment to record
    // the parking completion before the event arrives, so the wake is a real
    // wake and not an event consumed by the first activation.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // By workflow id alone: a caller holding a business key has no run id.
    client
        .get_workflow_handle(&workflow_id)
        .await
        .context("getting the parked workflow's handle by id")?
        .send_event(event_name, scenario.event_payload.clone())
        .await
        .context("sending the event")?;

    let result = match handle
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
        .await
    {
        Ok(result) => result,
        Err(e) => {
            // Do not leave a parked run behind on every execution of the suite;
            // the known-gap timeout scenario would otherwise strand one per run.
            let _ = handle.cancel().await;
            return Err(anyhow!("expected completion after the event but got: {e}"));
        }
    };

    if !json_matches(&expect.result, &result) {
        return Err(anyhow!(
            "result mismatch\n    expected: {}\n    actual:   {}",
            expect.result,
            result
        ));
    }
    within_bounds(&expect.result_at_most, &result)
}

/// Run one workflow-id reuse scenario.
///
/// Starts the workflow under a fresh id, then again under the same id while
/// the first run is open: that start must be refused with
/// `expect.error_code`, naming the open run. Once the first run completes, a
/// start that reuses an id only after a failure is refused the same way, and
/// a plain start runs the workflow again. A duplicate start while the first
/// run is open must never record a second run.
async fn run_workflow_id_reuse_scenario(
    client: &Client,
    args: &Args,
    scenario: &Scenario,
) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("workflow_id_reuse scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("workflow_id_reuse scenario missing `expect`"))?;
    let expected_code = expect
        .error_code
        .as_deref()
        .ok_or_else(|| anyhow!("workflow_id_reuse scenario missing `expect.error_code`"))?;

    let workflow_id = format!("conf-reuse-{}", fresh_key("id"));
    let start = |policy: Option<WorkflowIdReusePolicy>| {
        let mut options = StartWorkflowOptions::new(args.task_queue.clone())
            .with_workflow_id(workflow_id.clone())
            .with_namespace(args.namespace.clone());
        options.id_reuse_policy = policy;
        client.start_workflow_with_options(workflow, scenario.input.clone(), options)
    };
    // A refused start must carry the code, and name the run in the way.
    let refused = |outcome: orcher_sdk::Result<orcher_sdk::client::WorkflowHandle>,
                   in_the_way: &str,
                   when: &str|
     -> Result<()> {
        match outcome {
            Ok(handle) => Err(anyhow!(
                "{when}: the start was accepted as run {:?}",
                handle.run_id()
            )),
            Err(e) => {
                let code = e.code().to_string();
                if code != expected_code {
                    return Err(anyhow!(
                        "{when}: expected {expected_code}, got {code} ({e})"
                    ));
                }
                match e {
                    orcher_sdk::Error::Client(
                        orcher_sdk::error::ClientError::WorkflowAlreadyExists {
                            run_id: Some(ref run_id),
                            ..
                        },
                    ) if run_id == in_the_way => Ok(()),
                    other => Err(anyhow!(
                        "{when}: the refusal does not name run {in_the_way}: {other:?}"
                    )),
                }
            }
        }
    };
    let completes =
        |result: std::result::Result<Value, orcher_sdk::Error>, which: &str| -> Result<()> {
            let result = result.map_err(|e| anyhow!("the {which} run did not complete: {e}"))?;
            if json_matches(&expect.result, &result) {
                Ok(())
            } else {
                Err(anyhow!(
                    "the {which} run's result\n    expected: {}\n    actual:   {}",
                    expect.result,
                    result
                ))
            }
        };

    let first = start(None)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;
    let first_run = first
        .run_id()
        .ok_or_else(|| anyhow!("the first start returned no run id"))?
        .to_string();

    refused(
        start(None).await,
        &first_run,
        "a second start while it runs",
    )?;

    completes(
        first
            .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
            .await,
        "first",
    )?;

    refused(
        start(Some(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)).await,
        &first_run,
        "reusing only after a failure, once it completed",
    )?;

    let again = start(Some(WorkflowIdReusePolicy::AllowDuplicate))
        .await
        .context("starting it again once it completed")?;
    if again.run_id() == Some(first_run.as_str()) {
        return Err(anyhow!("the new start reported the first run's id"));
    }
    completes(
        again
            .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
            .await,
        "second",
    )
}

/// What running a scenario amounted to.
///
/// A scenario that could not run asserts nothing, so it is reported as skipped
/// rather than passed; otherwise the engine-restart check would read as green
/// wherever `ORCHER_CONTRACT_RESTART_CMD` is not set.
enum Outcome {
    Ran,
    Skipped(String),
}
/// Run one engine-restart scenario: restart the engine with the command in
/// `ORCHER_CONTRACT_RESTART_CMD`, then start a workflow and expect the worker,
/// untouched, to pick it up and complete it.
///
/// Skipped (reported, not failed) when the variable is unset: the harness has
/// no general way to restart an engine it did not start. This guards against
/// an engine restart leaving the worker's workflow pollers down for the rest
/// of the process while the worker keeps heartbeating and looks healthy.
async fn run_engine_restart_scenario(
    client: &Client,
    args: &Args,
    scenario: &Scenario,
) -> Result<Outcome> {
    let Ok(cmd) = std::env::var("ORCHER_CONTRACT_RESTART_CMD") else {
        return Ok(Outcome::Skipped(
            "ORCHER_CONTRACT_RESTART_CMD is not set".to_string(),
        ));
    };
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("engine_restart scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("engine_restart scenario missing `expect`"))?;

    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .status()
        .context("running the restart command")?;
    if !status.success() {
        return Err(anyhow!("restart command failed with {status}"));
    }
    // Let the worker notice and recover on its own. Recovery is the assertion,
    // so nothing here reconnects or restarts the worker.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let options = StartWorkflowOptions::new(args.task_queue.clone())
        .with_workflow_id(fresh_key("conf-restart"))
        .with_namespace(args.namespace.clone());
    let handle = client
        .start_workflow_with_options(workflow, scenario.input.clone(), options)
        .await
        .with_context(|| format!("starting workflow {workflow} after the restart"))?;
    let result = handle
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs))
        .await
        .context("the worker never picked the workflow up after the engine restart")?;

    if json_matches(&expect.result, &result) {
        Ok(Outcome::Ran)
    } else {
        Err(anyhow!(
            "result mismatch\n    expected: {}\n    actual:   {}",
            expect.result,
            result
        ))
    }
}

/// Spawn a worker process of this harness that serves `task_queue` alone.
/// When `doomed`, the crash scenario's task takes the process down.
fn spawn_worker(args: &Args, task_queue: &str, doomed: bool) -> Result<std::process::Child> {
    let mut command =
        std::process::Command::new(std::env::current_exe().context("finding this binary")?);
    if doomed {
        command.env("ORCHER_CONTRACT_DOOMED_WORKER", "1");
    } else {
        command.env_remove("ORCHER_CONTRACT_DOOMED_WORKER");
    }
    command
        .args([
            "--worker-only",
            "--server-url",
            &args.server_url,
            "--namespace",
            &args.namespace,
            "--task-queue",
            task_queue,
        ])
        .stdin(std::process::Stdio::null())
        .spawn()
        .context("starting a worker process")
}

/// Run one worker-crash scenario: start the workflow on a queue of its own,
/// served by a worker process its task kills; once that process is gone,
/// start another, and assert the workflow completes on it with `expect`.
///
/// The task sets no timeouts, so only the engine's default heartbeat timeout
/// (which applies because the worker heartbeats every running task) recovers
/// the attempt that died with its worker. The wait allows for an engine at its defaults: a
/// minute's timeout and a thirty-second sweep.
async fn run_worker_crash_scenario(
    client: &Client,
    args: &Args,
    scenario: &Scenario,
) -> Result<()> {
    let workflow = scenario
        .workflow
        .as_deref()
        .ok_or_else(|| anyhow!("worker_crash scenario missing `workflow`"))?;
    let expect = scenario
        .expect
        .as_ref()
        .ok_or_else(|| anyhow!("worker_crash scenario missing `expect`"))?;
    let task_queue = fresh_key("contract-crash");

    let mut doomed = spawn_worker(args, &task_queue, true)?;
    let options =
        StartWorkflowOptions::new(task_queue.clone()).with_namespace(args.namespace.clone());
    let handle = client
        .start_workflow_with_options(workflow, scenario.input.clone(), options)
        .await
        .with_context(|| format!("starting workflow {workflow}"))?;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = doomed.try_wait().context("waiting on the worker")? {
            break status;
        }
        if std::time::Instant::now() > deadline {
            let _ = doomed.kill();
            return Err(anyhow!("the task never took its worker down"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if status.success() {
        return Err(anyhow!(
            "the first worker exited cleanly ({status}), not mid-task"
        ));
    }

    let mut survivor = spawn_worker(args, &task_queue, false)?;
    let outcome = handle
        .result_with_timeout::<Value>(Duration::from_secs(args.result_timeout_secs + 120))
        .await;
    let _ = survivor.kill();
    let _ = survivor.wait();

    let result = outcome.context("the task was never retried after its worker died")?;
    if json_matches(&expect.result, &result) {
        Ok(())
    } else {
        Err(anyhow!(
            "result mismatch\n    expected: {}\n    actual:   {}",
            expect.result,
            result
        ))
    }
}

/// Structural match: every field in `expected` must be present and deep-equal in
/// `actual`. Extra fields in `actual` are allowed, so a scenario asserts only the
/// fields it cares about.
fn json_matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(exp), Value::Object(act)) => exp
            .iter()
            .all(|(k, v)| act.get(k).is_some_and(|av| json_matches(v, av))),
        (Value::Array(exp), Value::Array(act)) => {
            exp.len() == act.len() && exp.iter().zip(act).all(|(e, a)| json_matches(e, a))
        }
        _ => expected == actual,
    }
}
