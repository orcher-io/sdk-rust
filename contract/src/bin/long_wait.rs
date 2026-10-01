//! Checks that awaiting a workflow that runs longer than the server's default
//! result long-poll window succeeds.
//!
//! The server bounds each result long-poll (60s by default). `handle.result()`
//! re-issues the long-poll until the workflow reaches a terminal state, so a
//! wait of any length works rather than failing with DEADLINE_EXCEEDED while
//! the workflow is still healthy and running.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin long_wait -- --sleep-secs 75

use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use orcher::client::{ClientConfig, StartWorkflowOptions};
use orcher::prelude::*;

#[derive(serde::Serialize, serde::Deserialize)]
struct LongInput {
    sleep_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LongOutput {
    slept_secs: u64,
}

/// Sleeps inside the task body so the workflow really stays running past the
/// server's long-poll window, rather than merely being slow to dispatch.
#[task(name = "long_task")]
async fn long_task(_ctx: TaskContext, input: LongInput) -> Result<LongOutput> {
    tokio::time::sleep(Duration::from_secs(input.sleep_secs)).await;
    Ok(LongOutput {
        slept_secs: input.sleep_secs,
    })
}

#[workflow(name = "long_workflow")]
async fn long_workflow(ctx: WorkflowContext, input: LongInput) -> Result<LongOutput> {
    ctx.execute_task(long_task, input).await
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Await a workflow that outlives the server's default result window")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "long-wait")]
    task_queue: String,
    /// Must exceed the server's DEFAULT_RESULT_TIMEOUT_SECS (60) to be a real check.
    #[arg(long, default_value_t = 75)]
    sleep_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!(
        "long-wait check | workflow sleeps {}s (server default result window is 60s)",
        args.sleep_secs
    );

    let wa = args.clone();
    let worker = tokio::spawn(async move {
        let worker = Worker::builder()
            .server_url(&wa.server_url)
            .task_queue(&wa.task_queue)
            .namespace(&wa.namespace)
            .build()
            .await?;
        worker.run().await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = Client::with_config(
        ClientConfig::new(args.server_url.clone()).with_namespace(args.namespace.clone()),
    )
    .await
    .context("connecting client")?;

    let started = Instant::now();
    let handle = client
        .start_workflow_with_options(
            "long_workflow",
            LongInput {
                sleep_secs: args.sleep_secs,
            },
            StartWorkflowOptions::new(args.task_queue.clone())
                .with_namespace(args.namespace.clone()),
        )
        .await
        .context("starting workflow")?;

    // The assertion: no explicit timeout, so this must wait as long as the
    // workflow runs instead of being cut off at the server's 60s default.
    let out: LongOutput = handle.result().await.context(
        "awaiting result — a DEADLINE_EXCEEDED here means the result wait is still capped",
    )?;

    let elapsed = started.elapsed();
    worker.abort();

    println!("  slept_secs reported : {}", out.slept_secs);
    println!("  wall clock          : {:.1}s", elapsed.as_secs_f64());

    anyhow::ensure!(
        out.slept_secs == args.sleep_secs,
        "unexpected result payload"
    );
    anyhow::ensure!(
        elapsed.as_secs() >= args.sleep_secs,
        "returned too early ({:?}) — the workflow cannot have run to completion",
        elapsed
    );

    println!(
        "\nLONG-WAIT: PASS — awaited a {}s workflow past the 60s default window",
        args.sleep_secs
    );
    Ok(())
}
