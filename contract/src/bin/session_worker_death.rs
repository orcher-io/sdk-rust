//! Checks that a workflow learns its session is gone when the session's worker
//! dies (orcher-io/orcher#303).
//!
//! The workflow opens a session and runs one long task on it, catching the
//! task's failure. A worker process takes the session and starts the task;
//! the check kills that process outright, starts a healthy worker, and waits.
//! Only the dead worker polled the session's queue, so the task cannot be run
//! again there: the workflow must be told the task failed, and finish, within
//! the timeout. A workflow still running at the end is the hang.
//!
//! Usage (against a running engine; set the engine's
//! ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS low for a quick check):
//!   cargo run -p orcher-contract --bin session_worker_death -- --server-url http://localhost:50051

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;
use orcher_sdk::workflow::session::SessionOptions;

/// Runs for `secs` seconds; the worker running it is killed long before.
#[task]
async fn session_long_task(_ctx: TaskContext, secs: u64) -> Result<String> {
    tokio::time::sleep(Duration::from_secs(secs)).await;
    Ok("finished".to_string())
}

/// Opens a session, runs the long task on it, and reports how the task ended.
#[workflow(name = "session_worker_death")]
async fn session_worker_death(ctx: WorkflowContext, secs: u64) -> Result<String> {
    let mut session = ctx
        .create_session(SessionOptions::default().with_heartbeat_interval(Duration::from_secs(1)))
        .await?;
    let outcome = match session
        .execute_task::<_, _, String>(session_long_task, secs)
        .await
    {
        Ok(result) => format!("task completed: {result}"),
        Err(e) if e.is_suspend() => return Err(e),
        Err(e) => format!("task failed: {e}"),
    };
    session.complete();
    Ok(outcome)
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Kill a session's worker mid-task and check the workflow finishes")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "session-death")]
    task_queue: String,
    /// Seconds after starting the workflow before the session's worker is
    /// killed: long enough for it to have taken the session and its task.
    #[arg(long, default_value_t = 5)]
    kill_after_secs: u64,
    /// How long the workflow may take to finish once the worker is gone.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,
    /// Run only a worker (used by the check for its worker processes).
    #[arg(long)]
    worker: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.worker {
        let worker = Worker::builder()
            .server_url(&args.server_url)
            .task_queue(&args.task_queue)
            .namespace(&args.namespace)
            .build()
            .await?;
        return Ok(worker.run().await?);
    }

    let spawn_worker = || -> anyhow::Result<Child> {
        Command::new(std::env::current_exe()?)
            .args([
                "--worker",
                "--server-url",
                &args.server_url,
                "--namespace",
                &args.namespace,
                "--task-queue",
                &args.task_queue,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("starting a worker process")
    };

    let mut doomed = spawn_worker()?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = Client::with_config(
        ClientConfig::new(args.server_url.clone())
            .with_namespace(args.namespace.clone())
            .with_timeout(Duration::from_secs(args.timeout_secs + 10)),
    )
    .await
    .context("connecting client")?;
    let handle = client
        .start_workflow_with_options(
            "session_worker_death",
            3600u64,
            StartWorkflowOptions::new(args.task_queue.clone())
                .with_namespace(args.namespace.clone()),
        )
        .await
        .context("starting the workflow")?;

    tokio::time::sleep(Duration::from_secs(args.kill_after_secs)).await;
    doomed.kill().context("killing the session's worker")?;
    let _ = doomed.wait();
    let mut healthy = spawn_worker()?;

    let result = handle
        .result_with_timeout::<String>(Duration::from_secs(args.timeout_secs))
        .await;
    let _ = healthy.kill();
    let _ = healthy.wait();

    match result {
        Ok(outcome) => {
            println!("workflow finished after its session's worker died: {outcome}");
            Ok(())
        }
        Err(e) => {
            println!("workflow did not finish after its session's worker died: {e}");
            anyhow::bail!("session worker death left the workflow hanging")
        }
    }
}
