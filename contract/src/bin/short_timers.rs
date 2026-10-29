//! Checks that short timers work when the engine is busy (orcher-io/orcher#375).
//!
//! Each run sleeps `--sleeps` times for `--ms` milliseconds, and `--runs` run at
//! once. A timer is recorded after the engine has begun its transaction; when
//! that waits for a connection longer than the timer's duration, the timer is
//! already due when it is recorded. It must then fire at once, not fail the
//! run's activation. The check counts runs that finish within the timeout.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin short_timers -- --server-url http://localhost:50051

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;

/// Sleeps `ms` milliseconds `count` times and returns how many sleeps it took.
#[workflow(name = "short_timers")]
async fn short_timers(ctx: WorkflowContext, input: (u64, u64)) -> Result<u64> {
    let (count, ms) = input;
    for _ in 0..count {
        ctx.sleep(Duration::from_millis(ms)).await?;
    }
    Ok(count)
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Run many short timers at once and check every run finishes")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "short-timers")]
    task_queue: String,
    /// Runs started at once.
    #[arg(long, default_value_t = 50)]
    runs: u64,
    /// Sleeps each run takes, one after another.
    #[arg(long, default_value_t = 10)]
    sleeps: u64,
    /// Milliseconds each sleep lasts.
    #[arg(long, default_value_t = 10)]
    ms: u64,
    /// How long every run may take to finish.
    #[arg(long, default_value_t = 90)]
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
                "short_timers",
                (args.sleeps, args.ms),
                StartWorkflowOptions::new(args.task_queue.clone())
                    .with_namespace(args.namespace.clone()),
            )
            .await
            .with_context(|| format!("starting run {run}"))?;
        runs.push(handle);
    }

    let timeout = Duration::from_secs(args.timeout_secs);
    let results = futures::future::join_all(
        runs.iter()
            .map(|handle| handle.result_with_timeout::<u64>(timeout)),
    )
    .await;

    let mut finished = 0;
    let mut failed = 0;
    for result in results {
        match result {
            Ok(count) if count == args.sleeps => finished += 1,
            Ok(count) => {
                failed += 1;
                eprintln!("  finished after {count} sleeps");
            }
            Err(e) => {
                failed += 1;
                eprintln!("  did not finish: {e}");
            }
        }
    }
    println!(
        "{finished}/{} runs finished all {} sleeps of {} ms; {failed} did not",
        args.runs, args.sleeps, args.ms
    );
    worker.abort();
    if finished as u64 == args.runs {
        Ok(())
    } else {
        anyhow::bail!("short timers did not all fire")
    }
}
