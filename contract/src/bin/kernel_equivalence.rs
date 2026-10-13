//! Replays recorded workflow activations through both of the SDK's paths and
//! checks they produce the same thing.
//!
//! The SDK runs a workflow activation either from the journal the engine
//! sends (`ExecutionRuntime::execute_workflow`) or from the kernel
//! protocol's `RunActivation` (`ExecutionRuntime::run_activation`), which is
//! how the worker host hands activations over. For every activation recorded
//! from this catalog (`ORCHER_RECORD_ACTIVATIONS`), the kernel path's answer,
//! turned into what goes to the engine by sdk-core
//! (`kernel::execution_result`), must equal what the journal path produced:
//! the same commands, the same steps reached, the same query and update
//! answers, the same failure.
//!
//! Needs no engine. Usage: `kernel_equivalence <recorded dir>...`, each
//! holding runs recorded from the Rust contract suite.

#[path = "../catalog.rs"]
#[allow(dead_code)]
mod catalog;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use orcher_sdk::worker::{ExecutionRuntime, Registry, RuntimeConfig};
use orcher_sdk_core::kernel::{execution_result, resolve};
use orcher_sdk_core::worker::poll::polling::WorkflowExecutionTask;
use orcher_sdk_core::worker::record::{read_runs, Recorded};

const NAMESPACE: &str = "contract";

#[tokio::main]
async fn main() -> ExitCode {
    let dirs: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if dirs.is_empty() {
        eprintln!("usage: kernel_equivalence <recorded dir>...");
        return ExitCode::from(2);
    }
    let runtime = runtime().await;

    let (mut checked, mut problems) = (0usize, Vec::new());
    for dir in &dirs {
        let runs = match read_runs(dir) {
            Ok(runs) => runs,
            Err(error) => {
                eprintln!("cannot read {}: {error}", dir.display());
                return ExitCode::from(2);
            }
        };
        for run in runs {
            for message in &run.messages {
                let Recorded::Activation(response) = message else {
                    continue;
                };
                checked += 1;
                let task = WorkflowExecutionTask::from(response.clone());
                if let Err(problem) = compare(&runtime, task).await {
                    problems.push(format!(
                        "{} / {}: {problem}",
                        dir.display(),
                        response.workflow_type
                    ));
                }
            }
        }
    }

    for problem in &problems {
        eprintln!("DIFFERS {problem}\n");
    }
    println!(
        "{checked} activations replayed through both paths, {} differ",
        problems.len()
    );
    if checked == 0 || !problems.is_empty() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// A runtime with the whole catalog registered, as the contract worker has.
async fn runtime() -> Arc<ExecutionRuntime> {
    let mut config = RuntimeConfig::default();
    config.namespace = NAMESPACE.to_string();
    let runtime = Arc::new(ExecutionRuntime::new(config));
    let mut registry = Registry::new();
    registry.collect_from_global_registry();
    for name in registry.workflow_names() {
        if let Some(handler) = registry.get_workflow(name).and_then(|w| w.handler.clone()) {
            runtime
                .register_workflow_handler(name.to_string(), handler)
                .await;
        }
    }
    runtime
}

async fn compare(runtime: &ExecutionRuntime, task: WorkflowExecutionTask) -> Result<(), String> {
    let run_id = task.execution.run_id.clone();
    let activation = resolve(&task, NAMESPACE);
    // Some catalog workflows count their activations in the process; each
    // path sees the count as it stood before the activation.
    let counted = catalog::ACTIVATIONS.lock().unwrap().clone();
    let journal_path = runtime.execute_workflow(task).await;
    *catalog::ACTIVATIONS.lock().unwrap() = counted;
    let answer = runtime.run_activation(activation).await;
    let kernel_path = execution_result(&run_id, answer.clone())
        .map_err(|error| format!("the kernel refused the answer: {error}\nanswer: {answer:?}"))?;

    let mut journal_path = serde_json::to_value(&journal_path).unwrap();
    let kernel_path = serde_json::to_value(&kernel_path).unwrap();
    // A workflow failure, in the kernel protocol, carries no calls, so it
    // reports no steps reached; the journal path reports the steps reached
    // before the failure. Equivalent only when there were none, so only that
    // case is let through.
    if journal_path["successful"] == false && journal_path["reached_steps"] == serde_json::json!([])
    {
        journal_path
            .as_object_mut()
            .unwrap()
            .remove("reached_steps");
    }
    if journal_path == kernel_path {
        return Ok(());
    }
    Err(format!(
        "\n  journal path: {}\n  kernel path:  {}",
        serde_json::to_string(&journal_path).unwrap(),
        serde_json::to_string(&kernel_path).unwrap()
    ))
}
