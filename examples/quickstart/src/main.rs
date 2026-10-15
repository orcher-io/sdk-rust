//! # Orcher Quickstart
//!
//! The smallest complete durable workflow: two tasks wired into one workflow and
//! run end to end against a local orchestrator.
//!
//! Start an orchestrator on `localhost:50051` (or point `ORCHER_SERVER_URL` at
//! one elsewhere), then, from the repository root:
//!
//! ```bash
//! cargo run -p orcher-example-quickstart
//! ```
//!
//! This starts a worker in the background, runs one `process_order` workflow,
//! prints the result, and exits. In production the worker and the client that
//! starts workflows are usually separate processes (see the `durable_execution`
//! example); the quickstart runs both in one process to stay a single file.

use std::time::Duration;

use orcher_sdk::client::{ClientConfig, StartWorkflowOptions};
use orcher_sdk::prelude::*;
use serde::{Deserialize, Serialize};

const NAMESPACE: &str = "examples";
const TASK_QUEUE: &str = "quickstart";

// ============================================================================
// Tasks
// ============================================================================
// A task is an async fn annotated with `#[task]`. It runs on a worker and its
// result is journaled, so a workflow that replays after a crash never runs a
// completed task twice.

/// A task input can be any `Serialize`/`Deserialize` type. Prefer a named struct
/// over a positional tuple: the fields are self-documenting and type-checked.
#[derive(Serialize, Deserialize)]
struct ChargeRequest {
    amount: f64,
    currency: String,
}

#[derive(Serialize, Deserialize)]
struct Payment {
    charge_id: String,
    status: String,
}

/// `retry = 3` tells Orcher to retry this task up to three times if it returns
/// an error. A real implementation would call a payment gateway here.
#[task(name = "charge_payment", retry = 3)]
async fn charge_payment(_ctx: TaskContext, req: ChargeRequest) -> Result<Payment> {
    println!("  charge_payment: {:.2} {}", req.amount, req.currency);
    Ok(Payment {
        charge_id: "ch_12345".to_string(),
        status: "succeeded".to_string(),
    })
}

/// Tasks can also take a plain scalar input.
#[task(name = "reserve_inventory")]
async fn reserve_inventory(_ctx: TaskContext, order_id: String) -> Result<String> {
    println!("  reserve_inventory: {order_id}");
    Ok(format!("rsv_{order_id}"))
}

// ============================================================================
// Workflow
// ============================================================================
// A workflow orchestrates tasks. Every `execute_task` call is journaled, so if
// the workflow crashes and restarts, completed steps replay from the journal
// instead of executing again.

#[derive(Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct OrderOutput {
    order_id: String,
    charge_id: String,
    reservation_id: String,
}

#[workflow(name = "process_order")]
async fn process_order(ctx: WorkflowContext, input: OrderInput) -> Result<OrderOutput> {
    // Pass a typed function reference (`charge_payment`), not a string name, so a
    // typo or a wrong input type is a compile error rather than a runtime failure.
    let payment: Payment = ctx
        .execute_task(
            charge_payment,
            ChargeRequest {
                amount: input.amount,
                currency: "USD".to_string(),
            },
        )
        .await?;

    let reservation_id: String = ctx
        .execute_task(reserve_inventory, input.order_id.clone())
        .await?;

    Ok(OrderOutput {
        order_id: input.order_id,
        charge_id: payment.charge_id,
        reservation_id,
    })
}

// ============================================================================
// Run it
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let server_url =
        std::env::var("ORCHER_SERVER_URL").unwrap_or_else(|_| "http://localhost:50051".into());

    // Start a worker in the background. Tasks and workflows auto-register via the
    // `#[task]`/`#[workflow]` macros, so there is nothing to wire up by hand.
    let worker = Worker::builder()
        .server_url(&server_url)
        .task_queue(TASK_QUEUE)
        .namespace(NAMESPACE)
        .build()
        .await?;
    let worker_handle = tokio::spawn(async move { worker.run().await });

    // Connect a client and start one workflow.
    let client = Client::with_config(
        ClientConfig::new(&server_url)
            .with_namespace(NAMESPACE)
            .with_timeout(Duration::from_secs(30)),
    )
    .await?;

    let handle = client
        .start_workflow_with_options(
            "process_order",
            OrderInput {
                order_id: "ord_1".to_string(),
                amount: 99.99,
            },
            StartWorkflowOptions::new(TASK_QUEUE).with_namespace(NAMESPACE),
        )
        .await?;

    println!("started workflow {}", handle.id());

    // `result()` waits for the workflow to finish and deserializes its output.
    let output: OrderOutput = handle.result().await?;
    println!(
        "done: order={} charge={} reservation={}",
        output.order_id, output.charge_id, output.reservation_id
    );

    worker_handle.abort();
    Ok(())
}
