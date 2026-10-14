//! Replays recorded workflow activations and checks the SDK still answers
//! each the way it did when the result was recorded.
//!
//! `contract/replay/<corpus>/` holds runs recorded from this catalog with
//! `ORCHER_RECORD_ACTIVATIONS`, and `expected.json` beside them what the
//! SDK produced for each activation: the commands, the steps reached, the
//! query and update answers, or the failure, as sdk-core's
//! `kernel::execution_result` turns the SDK's answer into what goes to the
//! engine. The expected results were produced while the SDK still had its
//! journal-reading path too, and both paths agreed on every one.
//!
//! Needs no engine. `replay_check` checks every corpus; `replay_check
//! --bless` rewrites `expected.json` from what the SDK answers now, for a
//! change meant to alter what it sends.

#[path = "../catalog.rs"]
#[allow(dead_code)]
mod catalog;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use orcher_sdk::worker::{ExecutionRuntime, Registry, RuntimeConfig};
use orcher_sdk_core::kernel::{execution_result, resolve};
use orcher_sdk_core::worker::poll::polling::WorkflowExecutionTask;
use orcher_sdk_core::worker::record::{read_runs, Recorded};

const NAMESPACE: &str = "contract";

#[tokio::main]
async fn main() -> ExitCode {
    let bless = std::env::args().any(|arg| arg == "--bless");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("replay");
    let runtime = runtime().await;

    let (mut checked, mut problems) = (0usize, Vec::new());
    for corpus in corpora(&root) {
        let expected_path = corpus.join("expected.json");
        let expected: BTreeMap<String, serde_json::Value> = if bless {
            BTreeMap::new()
        } else {
            match std::fs::read(&expected_path) {
                Ok(bytes) => serde_json::from_slice(&bytes).expect("expected.json is not valid"),
                Err(error) => {
                    eprintln!("cannot read {}: {error}", expected_path.display());
                    return ExitCode::from(2);
                }
            }
        };
        let mut answered = BTreeMap::new();
        for run in read_runs(&corpus).expect("read the recorded runs") {
            for (index, message) in run.messages.iter().enumerate() {
                let Recorded::Activation(response) = message else {
                    continue;
                };
                checked += 1;
                let key = format!("{}/{}#{index}", run.workflow_id, run.run_id);
                let task = WorkflowExecutionTask::from(response.clone());
                let result = match answer(&runtime, task).await {
                    Ok(result) => result,
                    Err(problem) => {
                        problems.push(format!("{key} ({}): {problem}", response.workflow_type));
                        continue;
                    }
                };
                if !bless {
                    match expected.get(&key) {
                        Some(expected) if *expected == result => {}
                        Some(expected) => problems.push(format!(
                            "{key} ({}):\n  expected: {expected}\n  answered: {result}",
                            response.workflow_type
                        )),
                        None => problems.push(format!("{key}: no expected result")),
                    }
                }
                answered.insert(key, result);
            }
        }
        if bless {
            let json = serde_json::to_string_pretty(&answered).unwrap();
            std::fs::write(&expected_path, json + "\n").expect("write expected.json");
            println!(
                "blessed {} ({} activations)",
                expected_path.display(),
                answered.len()
            );
        } else if let Some(missing) = expected.keys().find(|key| !answered.contains_key(*key)) {
            problems.push(format!("{missing}: expected, but not in the corpus"));
        }
    }

    for problem in &problems {
        eprintln!("DIFFERS {problem}\n");
    }
    println!(
        "{checked} recorded activations replayed, {} differ",
        problems.len()
    );
    if checked == 0 || !problems.is_empty() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Every corpus under `root`, in order.
fn corpora(root: &Path) -> Vec<PathBuf> {
    let mut corpora: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("contract/replay")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect();
    corpora.sort();
    corpora
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

/// The SDK's answer to a recorded activation, as what goes to the engine.
async fn answer(
    runtime: &ExecutionRuntime,
    task: WorkflowExecutionTask,
) -> Result<serde_json::Value, String> {
    let run_id = task.execution.run_id.clone();
    // Some catalog workflows count their activations in the process; each
    // activation is answered as if it were the run's next.
    catalog::ACTIVATIONS.lock().unwrap().clear();
    let answer = runtime.run_activation(resolve(&task, NAMESPACE)).await;
    let result = execution_result(&run_id, answer)
        .map_err(|error| format!("the kernel refused the answer: {error}"))?;
    Ok(serde_json::to_value(&result).unwrap())
}
