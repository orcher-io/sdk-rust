//! Tenant isolation, observed through a real client against a real engine.
//!
//! The engine scopes almost every record to an organization, and the guard on
//! each read and write compares that owner with the caller's. Those guards
//! depend on two things the main suite does not exercise: that the engine
//! decides *who the caller is* from the credential rather than from what the
//! caller claims, and that every by-id path checks. The main contract suite
//! runs with no credentials, so every guard is a no-op there and a regression
//! in any of them would pass it.
//!
//! This runs one organization's worker and workflow, then has a second
//! organization try to read and change it, and checks that each attempt is
//! refused in a way that does not reveal the workflow exists.
//!
//! It needs an engine in `api-key` mode and these variables:
//!
//!   ORCHER_TENANCY_KEY_A / _B      keys belonging to organization A / B
//!   ORCHER_TENANCY_ORG_A / _B      those organizations' ids
//!   ORCHER_TENANCY_KEY_UNBOUND     a key with no organization and no delegation
//!   ORCHER_TENANCY_KEY_DELEGATE    a key with no organization that may delegate
//!                                  (the shape of a trusted gateway's key)
//!
//! Exits non-zero on the first isolation failure.

use std::time::Duration;

use anyhow::{bail, ensure, Context};
use clap::Parser;
use orcher::client::{ClientConfig, StartWorkflowOptions};
use orcher::error::ClientError;
use orcher::prelude::*;

#[derive(serde::Serialize, serde::Deserialize)]
struct Note {
    text: String,
}

#[task(name = "tenancy_echo_task")]
async fn tenancy_echo_task(_ctx: TaskContext, input: Note) -> Result<Note> {
    Ok(input)
}

#[workflow(name = "tenancy_echo")]
async fn tenancy_echo(ctx: WorkflowContext, input: Note) -> Result<Note> {
    ctx.execute_task(tenancy_echo_task, input).await
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Check that one organisation cannot see or change another's work")]
struct Args {
    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,
    #[arg(long, default_value = "tenancy")]
    namespace: String,
    #[arg(long, default_value = "tenancy")]
    task_queue: String,
    #[arg(long, env = "ORCHER_TENANCY_KEY_A")]
    key_a: String,
    #[arg(long, env = "ORCHER_TENANCY_KEY_B")]
    key_b: String,
    #[arg(long, env = "ORCHER_TENANCY_ORG_A")]
    org_a: String,
    #[arg(long, env = "ORCHER_TENANCY_ORG_B")]
    org_b: String,
    #[arg(long, env = "ORCHER_TENANCY_KEY_UNBOUND")]
    key_unbound: String,
    #[arg(long, env = "ORCHER_TENANCY_KEY_DELEGATE")]
    key_delegate: String,
}

async fn client(args: &Args, key: &str, organization_id: Option<&str>) -> anyhow::Result<Client> {
    let mut config = ClientConfig::new(args.server_url.clone())
        .with_namespace(args.namespace.clone())
        .with_timeout(Duration::from_secs(60))
        .with_api_key(key);
    config.organization_id = organization_id.map(str::to_string);
    Client::with_config(config)
        .await
        .context("connecting client")
}

/// The HTTP-equivalent status the SDK reports for a gRPC failure.
fn status_of(err: &orcher::Error) -> Option<u16> {
    match err {
        orcher::Error::Client(ClientError::ServerError { status, .. }) => Some(*status),
        _ => None,
    }
}

/// Refused as though the workflow did not exist. Forbidden would confirm the id
/// is real, which turns a guess into an enumeration.
fn is_not_found(err: &orcher::Error) -> bool {
    status_of(err) == Some(404) || err.code() == orcher::ErrorCode::WorkflowNotFound
}

fn is_permission_denied(err: &orcher::Error) -> bool {
    status_of(err) == Some(403)
}

/// Each check prints its own line, so a failure names the boundary that broke.
macro_rules! check {
    ($name:expr, $body:expr) => {{
        match $body {
            Ok(()) => println!("  PASS  {}", $name),
            Err(e) => {
                println!("  FAIL  {}: {:#}", $name, e);
                return Err(e);
            }
        }
    }};
}

async fn ensure_namespace(client: &Client, name: &str) -> anyhow::Result<()> {
    match client.create_namespace(name, 7).await {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("already exists") => Ok(()),
        Err(e) => Err(e).with_context(|| format!("creating namespace '{name}'")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("tenancy isolation | server {}", args.server_url);

    let alpha = client(&args, &args.key_a, None).await?;
    let beta = client(&args, &args.key_b, None).await?;

    // Namespaces belong to an organization, so each tenant creates its own.
    // This also checks that the namespace client sends credentials.
    check!(
        "each organisation can manage its own namespaces",
        async {
            ensure_namespace(&alpha, &args.namespace).await?;
            ensure_namespace(&beta, &args.namespace).await
        }
        .await
    );

    // Organization A's worker, authenticated as A.
    let wa = args.clone();
    let worker = tokio::spawn(async move {
        Worker::builder()
            .server_url(&wa.server_url)
            .task_queue(&wa.task_queue)
            .namespace(&wa.namespace)
            .api_key(&wa.key_a)
            .build()
            .await?
            .run()
            .await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    let workflow_id = format!(
        "tenancy-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    let handle_a = alpha
        .start_workflow_with_options(
            "tenancy_echo",
            Note {
                text: "belongs to A".into(),
            },
            StartWorkflowOptions::new(args.task_queue.clone())
                .with_workflow_id(workflow_id.clone())
                .with_namespace(args.namespace.clone()),
        )
        .await
        .context("organisation A starting its workflow")?;

    check!(
        "organisation A runs its own workflow to completion",
        async {
            let out: Note = handle_a
                .result_with_timeout(Duration::from_secs(60))
                .await
                .context("awaiting A's result")?;
            ensure!(
                out.text == "belongs to A",
                "unexpected result {:?}",
                out.text
            );
            Ok(())
        }
        .await
    );

    let handle_b = beta
        .get_workflow_handle(&workflow_id)
        .await
        .context("organisation B building a handle")?;

    check!(
        "B cannot read A's workflow status",
        async {
            match handle_b.status().await {
                Ok(status) => bail!("B read A's status: {status:?}"),
                Err(e) if is_not_found(&e) => Ok(()),
                Err(e) => bail!("expected not-found, got {e}"),
            }
        }
        .await
    );

    check!(
        "B cannot read A's workflow result",
        async {
            match handle_b
                .result_with_timeout::<Note>(Duration::from_secs(5))
                .await
            {
                Ok(note) => bail!("B read A's result: {:?}", note.text),
                Err(e) if is_not_found(&e) => Ok(()),
                Err(e) => bail!("expected not-found, got {e}"),
            }
        }
        .await
    );

    check!(
        "B cannot cancel A's workflow, and A's workflow is untouched",
        async {
            match handle_b.cancel().await {
                Ok(()) => bail!("B's cancel of A's workflow was accepted"),
                Err(e) if is_not_found(&e) => {}
                Err(e) => bail!("expected not-found, got {e}"),
            }
            let status = handle_a.status().await.context("A re-reading its status")?;
            ensure!(
                format!("{status:?}").to_lowercase().contains("complete"),
                "A's workflow is no longer completed after B's attempt: {status:?}"
            );
            Ok(())
        }
        .await
    );

    check!(
        "B's listing does not include A's workflow",
        async {
            let page = beta
                .list_workflows(
                    orcher_sdk_core::ListWorkflowsOptions::default().with_page_size(1000),
                )
                .await
                .context("B listing")?;
            ensure!(
                !page.executions.iter().any(|e| e.workflow_id == workflow_id),
                "A's workflow appears in B's listing"
            );
            Ok(())
        }
        .await
    );

    check!(
        "A's key cannot claim to act for B",
        async {
            let impostor = client(&args, &args.key_a, Some(&args.org_b)).await?;
            let handle = impostor.get_workflow_handle(&workflow_id).await?;
            match handle.status().await {
                Ok(_) => bail!("a key belonging to A was accepted while naming B"),
                Err(e) if is_permission_denied(&e) => Ok(()),
                Err(e) => bail!("expected permission-denied, got {e}"),
            }
        }
        .await
    );

    check!(
        "a key with no organisation cannot name one",
        async {
            let unbound = client(&args, &args.key_unbound, Some(&args.org_a)).await?;
            let handle = unbound.get_workflow_handle(&workflow_id).await?;
            match handle.status().await {
                Ok(_) => bail!("an ownerless, non-delegating key acted for organisation A"),
                Err(e) if is_permission_denied(&e) => Ok(()),
                Err(e) => bail!("expected permission-denied, got {e}"),
            }
        }
        .await
    );

    check!(
        "the delegating edge acts for the organisation it names",
        async {
            let edge = client(&args, &args.key_delegate, Some(&args.org_a)).await?;
            let handle = edge.get_workflow_handle(&workflow_id).await?;
            handle
                .status()
                .await
                .context("the delegating key could not read A's workflow on A's behalf")?;
            Ok(())
        }
        .await
    );

    worker.abort();
    println!("tenancy isolation: all checks passed");
    Ok(())
}
