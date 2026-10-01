//! Checks that a parent workflow can send an event to, and cancel, a child it
//! started, through the `ChildWorkflowHandle`.
//!
//! The event check also guards replay: the parent sleeps after sending, so its
//! code runs again before it reads the child's result, and the child reports
//! whether a second copy of the event arrived.
//!
//! Usage (against a running engine):
//!   cargo run -p orcher-contract --bin child_handle_ops -- --server-url http://localhost:50051

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use orcher::client::{ClientConfig, StartWorkflowOptions};
use orcher::error::{Error, WorkflowError};
use orcher::prelude::*;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct EventOutcome {
    got: String,
    duplicate: bool,
}

/// Waits for `go`, then gives a second copy a few seconds to arrive.
#[workflow(name = "child_handle_ops_receiver")]
async fn receiver(ctx: WorkflowContext, _input: serde_json::Value) -> Result<EventOutcome> {
    let got: String = ctx.wait_for_event("go").await?;
    let again: Option<String> = ctx
        .wait_for_event_with_timeout("go", Duration::from_secs(3))
        .await?;
    Ok(EventOutcome {
        got,
        duplicate: again.is_some(),
    })
}

/// Sends `go` to its child, sleeps so it is replayed, then returns the child's outcome.
#[workflow(name = "child_handle_ops_sender")]
async fn sender(ctx: WorkflowContext, _input: serde_json::Value) -> Result<EventOutcome> {
    let child = ctx
        .start_child_workflow("child_handle_ops_receiver", serde_json::json!({}))
        .await?;
    child.send_event("go", "now").await?;
    ctx.sleep(Duration::from_secs(1)).await?;
    child.result().await
}

/// Sleeps far longer than the check waits.
#[workflow(name = "child_handle_ops_sleeper")]
async fn sleeper(ctx: WorkflowContext, _input: serde_json::Value) -> Result<String> {
    ctx.sleep(Duration::from_secs(600)).await?;
    Ok("slept".to_string())
}

/// Starts a sleeping child, cancels it, and reports how the child ended.
#[workflow(name = "child_handle_ops_canceler")]
async fn canceler(ctx: WorkflowContext, _input: serde_json::Value) -> Result<String> {
    let child = ctx
        .start_child_workflow("child_handle_ops_sleeper", serde_json::json!({}))
        .await?;
    ctx.sleep(Duration::from_secs(1)).await?;
    child.cancel().await?;
    match child.result::<String>().await {
        Ok(result) => Ok(format!("completed: {result}")),
        Err(Error::Workflow(WorkflowError::ChildWorkflowFailed { .. })) => {
            Ok("canceled".to_string())
        }
        Err(e) => Err(e),
    }
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Send an event to, and cancel, a child workflow through its handle")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "child-handle-ops")]
    task_queue: String,
    /// How long each parent may take to finish.
    #[arg(long, default_value_t = 60)]
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

    let event = async {
        let handle = client
            .start_workflow_with_options(
                "child_handle_ops_sender",
                serde_json::json!({}),
                options(),
            )
            .await?;
        handle.result_with_timeout::<EventOutcome>(deadline).await
    }
    .await;
    let expected = EventOutcome {
        got: "now".to_string(),
        duplicate: false,
    };
    match event {
        Ok(outcome) if outcome == expected => println!("  send_event ok"),
        Ok(outcome) => {
            println!("  send_event FAILED: child saw {outcome:?}");
            failures.push("send_event");
        }
        Err(e) => {
            println!("  send_event FAILED: {e}");
            failures.push("send_event");
        }
    }

    let cancel = async {
        let handle = client
            .start_workflow_with_options(
                "child_handle_ops_canceler",
                serde_json::json!({}),
                options(),
            )
            .await?;
        handle.result_with_timeout::<String>(deadline).await
    }
    .await;
    match cancel {
        Ok(outcome) if outcome == "canceled" => println!("  cancel     ok"),
        Ok(outcome) => {
            println!("  cancel     FAILED: parent reported {outcome:?}");
            failures.push("cancel");
        }
        Err(e) => {
            println!("  cancel     FAILED: {e}");
            failures.push("cancel");
        }
    }

    worker.abort();

    if failures.is_empty() {
        println!("\nCHILD-HANDLE-OPS: PASS");
        Ok(())
    } else {
        anyhow::bail!("CHILD-HANDLE-OPS: FAIL ({})", failures.join(", "))
    }
}
