<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-rust/main/assets/banner.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-rust/main/assets/banner-light.svg">
    <img alt="ORCHER Rust SDK" src="https://raw.githubusercontent.com/orcher-io/sdk-rust/main/assets/banner.svg" width="100%">
  </picture>
</p>

<p align="center"><sub>Crash-proof workflows, written as plain async Rust.</sub></p>

<br />

<div>
  <a href="https://crates.io/crates/orcher-sdk"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fcrates.io%2Fapi%2Fv1%2Fcrates%2Forcher-sdk&query=%24.crate.max_version&prefix=v&style=flat-square&labelColor=0a0a0a&color=04B385&logo=rust&logoColor=white&label=crates.io&cacheSeconds=600" alt="crates.io"></a>
  <a href="https://docs.rs/orcher-sdk"><img src="https://img.shields.io/docsrs/orcher-sdk?style=flat-square&labelColor=0a0a0a&color=38BDF0&logo=docsdotrs&logoColor=white" alt="docs.rs"></a>
  <a href="https://github.com/orcher-io/sdk-rust/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/orcher-io/sdk-rust/ci.yml?branch=main&style=flat-square&labelColor=0a0a0a&color=04B385&logo=github&logoColor=white&label=CI" alt="CI"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-38BDF0?style=flat-square&labelColor=0a0a0a" alt="Apache 2.0"></a>
</div>

<br />

Write `async fn`s; ORCHER journals each step and resumes interrupted runs where they stopped.

- <img height="14" src="https://octicons-col.vercel.app/sync/38BDF0"> **Durable**: every step journaled; crashed runs resume on another worker
- <img height="14" src="https://octicons-col.vercel.app/code/38BDF0"> **Plain Rust**: `#[workflow]` and `#[task]` on async functions, no DSL
- <img height="14" src="https://octicons-col.vercel.app/history/38BDF0"> **Deterministic replay**: changed code is caught, not silently replayed
- <img height="14" src="https://octicons-col.vercel.app/iterations/38BDF0"> **Smart retries**: per-task policies and never-retry error types
- <img height="14" src="https://octicons-col.vercel.app/clock/38BDF0"> **Timers and events**: sleep for days or wait for an outside event
- <img height="14" src="https://octicons-col.vercel.app/git-branch/38BDF0"> **Child workflows**: compose and run workflows in parallel
- <img height="14" src="https://octicons-col.vercel.app/database/38BDF0"> **Actors**: stateful objects with a single writer per key
- <img height="14" src="https://octicons-col.vercel.app/beaker/38BDF0"> **Testing**: run workflows in memory with mocks and a fake clock
- <img height="14" src="https://octicons-col.vercel.app/shield-lock/38BDF0"> **Multi-tenant**: API keys, organizations and namespaces built in

<br />

### <img height="16" src="https://octicons-col.vercel.app/download/38BDF0"> Install

```toml
[dependencies]
orcher-sdk = "0.5"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

> [!NOTE]
> You need an ORCHER engine to run workflows against. The SDK is pre-1.0: the
> API may change between minor releases, and every breaking change is listed in
> [CHANGELOG.md](CHANGELOG.md).

<br />

### <img height="16" src="https://octicons-col.vercel.app/play/38BDF0"> Quick start

Define a task and a workflow that calls it. The worker finds every `#[workflow]`
and `#[task]` in the binary, registers them with the engine and runs them:

```rust
use orcher_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
struct Order {
    id: String,
    email: String,
}

#[task(retry = 3)]
async fn send_confirmation(_ctx: TaskContext, order: Order) -> Result<String> {
    Ok(format!("sent confirmation for {} to {}", order.id, order.email))
}

#[workflow(name = "confirm_order")]
async fn confirm_order(ctx: WorkflowContext, order: Order) -> Result<String> {
    ctx.execute_task(send_confirmation, order).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let worker = Worker::builder()
        .server_url("http://localhost:50051")
        .namespace("default")
        .task_queue("orders")
        .build()
        .await?;

    worker.run().await
}
```

Start it from any process and wait for the result:

```rust
use orcher_sdk::client::StartWorkflowOptions;
use orcher_sdk::prelude::*;

let client = Client::connect("http://localhost:50051").await?;
let order = Order {
    id: "order-1".to_string(),
    email: "ada@example.com".to_string(),
};
let handle = client
    .start_workflow_with_options("confirm_order", order, StartWorkflowOptions::new("orders"))
    .await?;
let receipt: String = handle.result().await?;
```

> [!TIP]
> Workflow code is replayed from its history every time it resumes, so it must
> be deterministic. Do I/O and anything else that touches the outside world in
> tasks, and use `ctx.time()`, `ctx.rand()` and `ctx.sleep()` in workflows.

<br />

### <img height="16" src="https://octicons-col.vercel.app/book/38BDF0"> Guide

<details>
<summary><b>Timers</b>: sleep for minutes or months</summary>

<br />

A timer is recorded by the engine, so no worker is busy while it runs, and a
restart doesn't reset it:

```rust
use std::time::Duration;

#[workflow(name = "trial")]
async fn trial(ctx: WorkflowContext, email: String) -> Result<()> {
    ctx.sleep(Duration::from_secs(14 * 24 * 60 * 60)).await?;
    ctx.execute_task(send_trial_ended, email).await
}
```

</details>

<details>
<summary><b>Events</b>: wait for something outside the workflow</summary>

<br />

A workflow can park until a named event arrives, optionally with a deadline:

```rust
use std::time::Duration;

#[workflow(name = "approval")]
async fn approval(ctx: WorkflowContext, request_id: String) -> Result<String> {
    let decision: Option<bool> = ctx
        .wait_for_event_with_timeout("approved", Duration::from_secs(3 * 24 * 60 * 60))
        .await?;
    Ok(match decision {
        Some(true) => format!("{request_id} approved"),
        Some(false) => format!("{request_id} rejected"),
        None => format!("{request_id} expired"),
    })
}
```

Send the event from a client:

```rust
let handle = client.get_workflow_handle("approval-42").await?;
handle.send_event("approved", true).await?;
```

</details>

<details>
<summary><b>Child workflows</b>: compose workflows from workflows</summary>

<br />

```rust
#[workflow(name = "ship_order")]
async fn ship_order(ctx: WorkflowContext, order_id: String) -> Result<String> {
    let label: String = ctx
        .execute_child_workflow("print_label", order_id.clone())
        .await?;
    Ok(format!("{order_id} shipped with {label}"))
}
```

</details>

<details>
<summary><b>Retries</b>: decide which failures are worth retrying</summary>

<br />

Return a `TaskError::application` naming the failure's type, and list the types
never to retry on the task. A `TaskError::non_retryable` failure is never
retried, whatever the policy allows:

```rust
use orcher_sdk::error::TaskError;

#[task(retry = 5, non_retryable_errors = ["CardDeclined"])]
async fn charge(_ctx: TaskContext, order_id: String) -> Result<String> {
    // Runs once:
    return Err(TaskError::application("CardDeclined", "card declined").into());
    // Also runs once, listed or not:
    // return Err(TaskError::non_retryable("AccountClosed", "account closed").into());
}
```

Any other error is retried. `RetryPolicy::with_non_retryable_errors` sets the
same list on a policy built in code.

</details>

<details>
<summary><b>Large payloads</b>: what fits, and what happens when it doesn't</summary>

<br />

Workers and clients send and receive gRPC messages of up to 32 MiB; set
`ORCHER_MAX_MESSAGE_BYTES` to change that. The engine accepts one payload (an
input, a result, an event) of up to 8 MiB unless configured otherwise. A task
whose result is too large fails straight away with a `PayloadTooLarge` failure
that says how large it was, and is not retried: store large data elsewhere and
pass a reference.

</details>

<details>
<summary><b>Time and randomness</b>: the replay-safe way</summary>

<br />

`ctx.time()` reads the time the engine recorded, and `ctx.rand()` is seeded from
the run, so both give the same answer every time the workflow is replayed:

```rust
#[workflow(name = "invoice")]
async fn invoice(ctx: WorkflowContext, customer: String) -> Result<String> {
    let number = ctx.rand().uuid();
    let issued_at = ctx.time().now();
    Ok(format!("invoice {number} for {customer}, issued at {issued_at}"))
}
```

</details>

<details>
<summary><b>Testing</b>: run workflows in memory</summary>

<br />

```rust
use orcher_sdk::testing::TestEnv;

#[tokio::test]
async fn doubles() -> Result<()> {
    let mut env = TestEnv::new();
    env.register_workflow("double", |n: i64| async move { Ok(n * 2) });

    let result: i64 = env.execute_workflow("double", 21).await?;
    assert_eq!(result, 42);
    Ok(())
}
```

</details>

<br />

### <img height="16" src="https://octicons-col.vercel.app/package/38BDF0"> Crates and features

| Crate | What it is |
|-------|------------|
| [`orcher-sdk`](https://crates.io/crates/orcher-sdk) | The SDK: workflows, tasks, actors, the worker and the client |
| [`orcher-sdk-macros`](https://crates.io/crates/orcher-sdk-macros) | `#[workflow]`, `#[task]`, `#[actor]` and `#[derive(Payload)]`, re-exported by `orcher-sdk` |

Depend on `orcher-sdk` alone; the code the macros generate needs nothing else.

| Feature | Default | What it enables |
|---------|:-------:|-----------------|
| `full` | ✓ | `auto-register`, `derive` and `testing` |
| `auto-register` | via `full` | Registers every `#[workflow]` and `#[task]` with the worker |
| `derive` | via `full` | `#[derive(Payload)]` |
| `testing` | via `full` | In-memory test environment, task mocks and a mock clock |
| `compression` | | Gzip payload compression |

<br />

### <img height="16" src="https://octicons-col.vercel.app/heart/38BDF0"> Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md)
for how to build, test and propose a change.

### <img height="16" src="https://octicons-col.vercel.app/law/38BDF0"> License

Licensed under the [Apache License, Version 2.0](LICENSE).

<sub>The Rust logo is a trademark of the Rust Foundation, shown here to indicate the language this SDK is for.</sub>
