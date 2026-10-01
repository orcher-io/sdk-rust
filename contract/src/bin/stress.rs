//! Dispatch stress check.
//!
//! Drives many workflows concurrently through a live engine and asserts the core
//! dispatch invariants that unit tests can't see:
//!   - **exactly-once execution**: each task's body runs exactly once (no
//!     duplicate dispatch), and
//!   - **no stranded tasks**: every scheduled task runs.
//!
//! Each stress task records every execution of its body in an in-process ledger
//! keyed by a unique per-workflow id. A correct dispatcher yields exactly one
//! execution per key; a duplicate shows count > 1, a stranded task shows a missing
//! key. It also reports throughput (tasks/sec).
//!
//! Usage (against a running engine): cargo run -p orcher-contract --bin stress -- --count 500

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
// The prelude brings `orcher_sdk::Result`, which the #[task]/#[workflow] macros
// require. Keep it after other imports so it shadows any anyhow::Result.
use orcher_sdk::prelude::*;

/// Execution ledger: unique key -> number of times the task body ran.
/// Populated inside the worker process by every `stress_task` execution.
static LEDGER: LazyLock<Mutex<HashMap<String, u32>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(serde::Serialize, serde::Deserialize)]
struct StressInput {
    key: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StressOutput {
    ok: bool,
}

/// A trivial task that records that its body ran, keyed by the workflow's unique
/// id. Recording is deliberately not idempotent, so a task that runs more than
/// once shows as a count > 1.
#[task(name = "stress_task")]
async fn stress_task(_ctx: TaskContext, input: StressInput) -> Result<StressOutput> {
    *LEDGER.lock().unwrap().entry(input.key).or_insert(0) += 1;
    Ok(StressOutput { ok: true })
}

#[workflow(name = "stress_workflow")]
async fn stress_workflow(ctx: WorkflowContext, input: StressInput) -> Result<StressOutput> {
    ctx.execute_task(stress_task, input).await
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FanOutInput {
    key: String,
    children: usize,
}

/// Starts `children` children at once, then waits for every one.
///
/// Siblings finishing together append their outcomes to one parent journal at
/// the same moment, which is where a lost append hides: the parent never sees
/// that child finish and never completes, so it shows up here as a workflow
/// that did not complete.
#[workflow(name = "stress_fan_out")]
async fn stress_fan_out(ctx: WorkflowContext, input: FanOutInput) -> Result<StressOutput> {
    let mut children = Vec::with_capacity(input.children);
    for j in 0..input.children {
        let key = format!("{}-{j}", input.key);
        children.push(
            ctx.start_child_workflow("stress_workflow", StressInput { key })
                .await?,
        );
    }
    for child in children {
        let _: StressOutput = child.result().await?;
    }
    Ok(StressOutput { ok: true })
}

#[derive(Parser, Debug, Clone)]
#[command(about = "ORCHER dispatch stress harness — exactly-once under concurrency")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "dispatch-stress")]
    task_queue: String,
    /// Number of concurrent workflows (each runs one stress task).
    #[arg(long, default_value_t = 200)]
    count: usize,
    /// Children per workflow. With 0 each workflow runs the stress task
    /// itself; with N it starts N child workflows at once, each running one.
    #[arg(long, default_value_t = 0)]
    fan_out: usize,
    /// Seconds to wait for all workflows to reach a terminal state.
    #[arg(long, default_value_t = 60)]
    deadline_secs: u64,
    /// Worker workflow-poller count (drains start + resume activations).
    #[arg(long, default_value_t = 8)]
    workflow_pollers: usize,
    /// Worker task-poller count (drains scheduled tasks).
    #[arg(long, default_value_t = 8)]
    task_pollers: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();

    // One stress task runs per workflow, or per child when fanning out.
    let expected_tasks = args.count * args.fan_out.max(1);
    println!(
        "ORCHER dispatch stress | {} concurrent workflows | {} | server {}",
        args.count,
        if args.fan_out == 0 {
            "no children".to_string()
        } else {
            format!("{} children each", args.fan_out)
        },
        args.server_url
    );

    // Run the worker in the background; the catalog registers itself.
    let wa = args.clone();
    let worker = tokio::spawn(async move {
        let worker = Worker::builder()
            .server_url(&wa.server_url)
            .task_queue(&wa.task_queue)
            .namespace(&wa.namespace)
            .workflow_poller_count(wa.workflow_pollers)
            .task_poller_count(wa.task_pollers)
            .build()
            .await?;
        worker.run().await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = Client::with_config(
        ClientConfig::new(args.server_url.clone())
            .with_namespace(args.namespace.clone())
            .with_timeout(Duration::from_secs(args.deadline_secs + 10)),
    )
    .await
    .context("connecting client")?;

    // Fire all workflows concurrently, each with a unique key, and await results.
    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.count);
    for i in 0..args.count {
        let client = client.clone();
        let opts = StartWorkflowOptions::new(args.task_queue.clone())
            .with_namespace(args.namespace.clone());
        let key = format!("stress-{i}");
        let fan_out = args.fan_out;
        let deadline = Duration::from_secs(args.deadline_secs);
        handles.push(tokio::spawn(async move {
            let handle = if fan_out == 0 {
                client
                    .start_workflow_with_options("stress_workflow", StressInput { key }, opts)
                    .await?
            } else {
                client
                    .start_workflow_with_options(
                        "stress_fan_out",
                        FanOutInput {
                            key,
                            children: fan_out,
                        },
                        opts,
                    )
                    .await?
            };
            handle.result_with_timeout::<StressOutput>(deadline).await
        }));
    }

    let mut completed = 0usize;
    let mut errors = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(_)) => completed += 1,
            _ => errors += 1,
        }
    }
    let elapsed = started.elapsed();
    worker.abort();

    // ---- Assert dispatch invariants against the ledger ----
    let ledger = LEDGER.lock().unwrap();
    let duplicates: Vec<(&String, &u32)> = ledger.iter().filter(|(_, &c)| c > 1).collect();
    let ran = ledger.len();
    let total_executions: u32 = ledger.values().sum();
    let stranded = expected_tasks.saturating_sub(ran);

    let tps = args.count as f64 / elapsed.as_secs_f64();
    println!(
        "\n  workflows completed : {completed}/{} (errors {errors})",
        args.count
    );
    println!("  distinct tasks ran  : {ran}/{expected_tasks}");
    println!("  total task executions: {total_executions} (exactly-once => {expected_tasks})");
    println!("  duplicate executions : {}", duplicates.len());
    println!("  stranded (never ran) : {stranded}");
    println!(
        "  wall clock          : {:.2}s  (~{:.0} workflows/sec)",
        elapsed.as_secs_f64(),
        tps
    );

    let mut failed = false;
    if completed != args.count {
        println!(
            "  FAIL: {} workflow(s) did not complete",
            args.count - completed
        );
        failed = true;
    }
    if !duplicates.is_empty() {
        let sample: Vec<String> = duplicates
            .iter()
            .take(5)
            .map(|(k, c)| format!("{k}={c}"))
            .collect();
        println!("  FAIL: duplicate task execution (dispatch not exactly-once): {sample:?}");
        failed = true;
    }
    if stranded > 0 {
        println!("  FAIL: {stranded} task(s) never executed (stranded)");
        failed = true;
    }

    if failed {
        println!("\nDISPATCH STRESS: FAIL");
        std::process::exit(1);
    }
    println!(
        "\nDISPATCH STRESS: PASS — exactly-once under {} concurrent workflows",
        args.count
    );
    Ok(())
}
