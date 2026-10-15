//! Worker mode implementation - polls for tasks and executes workflows

use anyhow::Result;
use orcher_sdk::prelude::*;
use tracing::info;

use super::args::Args;

// ============================================================================
// Worker Mode
// ============================================================================

/// Run in worker mode — polls for workflow and task executions.
pub async fn run(args: &Args) -> Result<()> {
    info!("🔧 Starting Durable Execution Worker");
    info!("   Server: {}", args.server_url);
    info!("   Task Queue: {}", args.task_queue);
    info!("   Namespace: {}", args.namespace);
    info!("   Workflow Pollers: {}", args.workflow_poller_count);
    info!("   Task Pollers: {}", args.task_poller_count);

    let mut builder = Worker::builder()
        .server_url(&args.server_url)
        .task_queue(&args.task_queue)
        .namespace(&args.namespace)
        .workflow_poller_count(args.workflow_poller_count)
        .task_poller_count(args.task_poller_count);
    if let Some(worker_id) = &args.worker_id {
        builder = builder.identity(worker_id);
    }
    let worker = builder.build().await?;

    info!("✅ Worker registered and ready");

    worker.run().await?;

    Ok(())
}
