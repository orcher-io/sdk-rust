//! Checks that events sent before a run waits for them are delivered when it
//! does (orcher-io/orcher#376).
//!
//! Each run waits for `--events` events in turn. The client sends all of them
//! as soon as the run is started, once each, so most arrive before the run
//! reaches the wait that reads them. A run that has its events journaled and is
//! never woken to read them stalls; the check counts runs that finish, with
//! every event received in the order sent, within the timeout.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin events_before_wait -- --server-url http://localhost:50051

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;

/// Waits for `count` `tick` events in turn and returns the payloads received.
#[workflow(name = "events_before_wait")]
async fn events_before_wait(ctx: WorkflowContext, count: u64) -> Result<Vec<u64>> {
    let mut received = Vec::new();
    for _ in 0..count {
        let tick: u64 = ctx.wait_for_event("tick").await?;
        received.push(tick);
    }
    Ok(received)
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Send every event before the run waits for it, and check every run finishes")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "events-before-wait")]
    task_queue: String,
    /// Runs started at once.
    #[arg(long, default_value_t = 20)]
    runs: u64,
    /// Events each run waits for, all sent up front.
    #[arg(long, default_value_t = 20)]
    events: u64,
    /// Milliseconds between one round of events (one to every run) and the
    /// next. 0 sends every event at once, before any run can reach its wait;
    /// a pause sends them while runs are mid-activation, the case where an
    /// event lands as the run's current turn is being applied. Either way
    /// each event is sent once and never again.
    #[arg(long, default_value_t = 0)]
    every_ms: u64,
    /// How long every run may take to finish.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

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
        ClientConfig::new(args.server_url.clone())
            .with_namespace(args.namespace.clone())
            .with_timeout(Duration::from_secs(args.timeout_secs + 10)),
    )
    .await
    .context("connecting client")?;

    let mut runs = Vec::new();
    for run in 0..args.runs {
        let handle = client
            .start_workflow_with_options(
                "events_before_wait",
                args.events,
                StartWorkflowOptions::new(args.task_queue.clone())
                    .with_namespace(args.namespace.clone()),
            )
            .await
            .with_context(|| format!("starting run {run}"))?;
        runs.push(handle);
    }
    // Each event once: all at once, or a round every `every_ms`.
    for tick in 0..args.events {
        for handle in &runs {
            handle
                .send_event("tick", tick)
                .await
                .with_context(|| format!("sending tick {tick}"))?;
        }
        if args.every_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.every_ms)).await;
        }
    }

    let expected: Vec<u64> = (0..args.events).collect();
    let timeout = Duration::from_secs(args.timeout_secs);
    let results = futures::future::join_all(
        runs.iter()
            .map(|handle| handle.result_with_timeout::<Vec<u64>>(timeout)),
    )
    .await;

    let mut finished = 0;
    let mut out_of_order = 0;
    let mut stalled = 0;
    for result in results {
        match result {
            Ok(received) if received == expected => finished += 1,
            Ok(received) => {
                out_of_order += 1;
                eprintln!("  received out of order: {received:?}");
            }
            Err(e) => {
                stalled += 1;
                eprintln!("  did not finish: {e}");
            }
        }
    }
    println!(
        "{finished}/{} runs received all {} events in order; {stalled} did not finish, \
         {out_of_order} out of order",
        args.runs, args.events
    );
    worker.abort();
    if finished as u64 == args.runs {
        Ok(())
    } else {
        anyhow::bail!("events sent before the wait were not all delivered")
    }
}
