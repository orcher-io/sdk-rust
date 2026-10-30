//! Checks that workers dying with long polls open do not stall other work
//! (orcher-io/orcher#285).
//!
//! Idle worker processes long-poll one task queue; a worker in this process
//! runs a steady stream of short workflows on another. Part way through, the
//! idle processes are killed outright, polls open. The check measures how long
//! each short workflow takes from start to result, before and after the kill:
//! killed pollers should be let go without holding up an unrelated queue.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin poller_disconnect -- --server-url http://localhost:50051

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;

/// Returns its input.
#[workflow(name = "poller_disconnect_echo")]
async fn poller_disconnect_echo(_ctx: WorkflowContext, input: u64) -> Result<u64> {
    Ok(input)
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Kill idle long-polling workers and measure an unrelated queue")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    /// The queue the killed workers poll.
    #[arg(long, default_value = "poller-disconnect-idle")]
    idle_queue: String,
    /// The queue the measured workflows run on.
    #[arg(long, default_value = "poller-disconnect-steady")]
    steady_queue: String,
    /// Idle worker processes, killed part way through.
    #[arg(long, default_value_t = 4)]
    idle_workers: usize,
    /// Seconds of measurement before the kill.
    #[arg(long, default_value_t = 10)]
    before_secs: u64,
    /// Seconds of measurement after the kill.
    #[arg(long, default_value_t = 75)]
    after_secs: u64,
    /// Milliseconds between measured workflows.
    #[arg(long, default_value_t = 200)]
    every_ms: u64,
    /// The slowest a measured workflow may take after the kill.
    #[arg(long, default_value_t = 5000)]
    max_ms: u64,
    /// Run only an idle worker on the idle queue (used by the check).
    #[arg(long)]
    idle: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let worker_on = |queue: &str| {
        Worker::builder()
            .server_url(&args.server_url)
            .task_queue(queue)
            .namespace(&args.namespace)
    };
    if args.idle {
        let worker = worker_on(&args.idle_queue).build().await?;
        return Ok(worker.run().await?);
    }

    let mut idle: Vec<Child> = (0..args.idle_workers)
        .map(|_| {
            Command::new(std::env::current_exe()?)
                .args([
                    "--idle",
                    "--server-url",
                    &args.server_url,
                    "--namespace",
                    &args.namespace,
                    "--idle-queue",
                    &args.idle_queue,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .context("starting an idle worker")
        })
        .collect::<anyhow::Result<_>>()?;

    let steady = worker_on(&args.steady_queue).build().await?;
    let steady = tokio::spawn(async move { steady.run().await });
    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = Client::with_config(
        ClientConfig::new(args.server_url.clone())
            .with_namespace(args.namespace.clone())
            .with_timeout(Duration::from_secs(120)),
    )
    .await
    .context("connecting client")?;

    let measure = |seconds: u64| {
        let client = &client;
        let args = &args;
        async move {
            let until = Instant::now() + Duration::from_secs(seconds);
            let mut runs = Vec::new();
            let mut n = 0u64;
            while Instant::now() < until {
                let client = client.clone();
                let options = StartWorkflowOptions::new(args.steady_queue.clone())
                    .with_namespace(args.namespace.clone());
                runs.push(tokio::spawn(async move {
                    let started = Instant::now();
                    let handle = client
                        .start_workflow_with_options("poller_disconnect_echo", n, options)
                        .await?;
                    handle
                        .result_with_timeout::<u64>(Duration::from_secs(110))
                        .await?;
                    Ok::<_, orcher_sdk::Error>(started.elapsed())
                }));
                n += 1;
                tokio::time::sleep(Duration::from_millis(args.every_ms)).await;
            }
            runs
        }
    };

    let before = measure(args.before_secs).await;
    for child in &mut idle {
        child.kill().context("killing an idle worker")?;
        let _ = child.wait();
    }
    let killed_at = Instant::now();
    let after = measure(args.after_secs).await;

    let summarize = |label: &str, runs: Vec<std::result::Result<Duration, String>>| {
        let mut ok: Vec<u128> = runs
            .iter()
            .filter_map(|r| r.as_ref().ok().map(|d| d.as_millis()))
            .collect();
        ok.sort_unstable();
        let failed = runs.len() - ok.len();
        let p = |q: f64| {
            ok.get(((ok.len() as f64 - 1.0) * q) as usize)
                .copied()
                .unwrap_or(0)
        };
        println!(
            "{label}: {} workflows, {failed} failed; p50 {} ms, p99 {} ms, max {} ms",
            runs.len(),
            p(0.5),
            p(0.99),
            ok.last().copied().unwrap_or(0)
        );
        (ok.last().copied().unwrap_or(0), failed)
    };
    let collect = |runs: Vec<
        tokio::task::JoinHandle<std::result::Result<Duration, orcher_sdk::Error>>,
    >| async {
        let mut out = Vec::new();
        for run in runs {
            out.push(match run.await {
                Ok(Ok(d)) => Ok(d),
                Ok(Err(e)) => Err(e.to_string()),
                Err(e) => Err(e.to_string()),
            });
        }
        out
    };
    summarize("before the kill", collect(before).await);
    let (worst, failed) = summarize("after the kill", collect(after).await);
    println!(
        "killed {} idle workers {:.0} s before the end",
        args.idle_workers,
        killed_at.elapsed().as_secs_f64()
    );
    steady.abort();
    if failed == 0 && worst <= args.max_ms as u128 {
        Ok(())
    } else {
        anyhow::bail!("workers dying with polls open held up another queue")
    }
}
