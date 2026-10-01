//! Checks that a `WorkflowHandle` can query a running workflow, send it an
//! event, and cancel it.
//!
//! Each operation is driven through the handle `start_workflow` returns, against
//! a real engine, and judged only by what a client can observe: the query's
//! answer, the result the event produces, and the status after cancellation.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin handle_ops -- --server-url http://localhost:50051

use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use orcher::client::{ClientConfig, StartWorkflowOptions};
use orcher::prelude::*;

/// Answers the `phase` query, then waits for a `go` event and returns its payload.
#[workflow(name = "handle_ops_wait")]
async fn handle_ops_wait(ctx: WorkflowContext, _input: serde_json::Value) -> Result<String> {
    ctx.register_query_handler("phase", || "waiting".to_string());
    let go: String = ctx.wait_for_event("go").await?;
    Ok(format!("got {go}"))
}

/// Sleeps long enough that only a cancellation ends it within the check.
#[workflow(name = "handle_ops_sleep")]
async fn handle_ops_sleep(ctx: WorkflowContext, _input: serde_json::Value) -> Result<String> {
    ctx.sleep(Duration::from_secs(600)).await?;
    Ok("slept".to_string())
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Query, send an event to, and cancel a workflow through its handle")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "handle-ops")]
    task_queue: String,
    /// How long each operation may take to show its effect.
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let deadline = Duration::from_secs(args.timeout_secs);

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
    let options = || {
        StartWorkflowOptions::new(args.task_queue.clone()).with_namespace(args.namespace.clone())
    };

    let mut failures = Vec::new();

    // Query, then event.
    let handle = client
        .start_workflow_with_options("handle_ops_wait", serde_json::json!({}), options())
        .await
        .context("starting handle_ops_wait")?;

    let started = Instant::now();
    let mut last_error = String::new();
    let mut answered = false;
    while started.elapsed() < deadline {
        match handle.query::<_, String>("phase", ()).await {
            Ok(phase) if phase == "waiting" => {
                answered = true;
                break;
            }
            Ok(other) => last_error = format!("answered {other:?}"),
            Err(e) => last_error = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    report(&mut failures, "query", answered, &last_error);

    let sent = handle.send_event("go", "now").await;
    let event_ok = match &sent {
        Ok(()) => match handle.result_with_timeout::<String>(deadline).await {
            Ok(result) if result == "got now" => Ok(()),
            Ok(result) => Err(format!("workflow returned {result:?}")),
            Err(e) => Err(format!("awaiting result: {e}")),
        },
        Err(e) => Err(format!("send_event: {e}")),
    };
    report(
        &mut failures,
        "send_event",
        event_ok.is_ok(),
        event_ok.as_ref().err().map_or("", String::as_str),
    );

    // Cancel.
    let handle = client
        .start_workflow_with_options("handle_ops_sleep", serde_json::json!({}), options())
        .await
        .context("starting handle_ops_sleep")?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let cancel_ok = match handle.cancel().await {
        Ok(()) => {
            let started = Instant::now();
            let mut status = Err("no status read".to_string());
            while started.elapsed() < deadline {
                status = handle.status().await.map_err(|e| e.to_string());
                if matches!(status, Ok(WorkflowStatus::Cancelled)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            match status {
                Ok(WorkflowStatus::Cancelled) => Ok(()),
                other => Err(format!("status after cancel: {other:?}")),
            }
        }
        Err(e) => Err(format!("cancel: {e}")),
    };
    report(
        &mut failures,
        "cancel",
        cancel_ok.is_ok(),
        cancel_ok.as_ref().err().map_or("", String::as_str),
    );

    worker.abort();

    if failures.is_empty() {
        println!("\nHANDLE-OPS: PASS");
        Ok(())
    } else {
        anyhow::bail!("HANDLE-OPS: FAIL ({})", failures.join(", "))
    }
}

fn report(failures: &mut Vec<&'static str>, name: &'static str, ok: bool, detail: &str) {
    if ok {
        println!("  {name:<10} ok");
    } else {
        println!("  {name:<10} FAILED: {detail}");
        failures.push(name);
    }
}
