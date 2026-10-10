//! A workflow or task that takes no input after its context compiles and runs: its
//! handler calls it with the context alone instead of failing when it runs.
#![cfg(feature = "auto-register")]

use orcher_sdk::payload::{from_payload, to_payload};
use orcher_sdk::worker::Registry;
use orcher_sdk::workflow::WorkflowExecution;
use orcher_sdk::{task, workflow, Result, TaskContext, WorkflowContext};

#[workflow]
async fn no_input_workflow(ctx: WorkflowContext) -> Result<String> {
    Ok(format!("ran {}", ctx.workflow_id()))
}

#[task]
async fn no_input_task(_ctx: TaskContext) -> Result<u32> {
    Ok(7)
}

fn context() -> WorkflowContext {
    let execution = WorkflowExecution::new(
        "wf-no-input".to_string(),
        "run-1".to_string(),
        "no_input_workflow".to_string(),
        "default".to_string(),
        "q".to_string(),
    );
    WorkflowContext::new(execution, false, "1".to_string())
}

#[tokio::test]
async fn a_workflow_without_input_runs_with_its_context_alone() {
    let mut registry = Registry::new();
    registry.collect_from_global_registry();
    let handler = registry.workflows["no_input_workflow"]
        .handler
        .clone()
        .expect("the macro registers a handler");

    // Whatever input the caller sent, empty or not, is not read.
    for input in [to_payload(&()).unwrap(), to_payload(&"ignored").unwrap()] {
        let output = handler(context(), input).await.expect("the workflow ran");
        let result: String = from_payload(&output).unwrap();
        assert_eq!(result, "ran wf-no-input");
    }
}

#[test]
fn a_task_without_input_is_registered_with_a_handler() {
    let mut registry = Registry::new();
    registry.collect_from_global_registry();
    assert!(registry.tasks["no_input_task"].handler.is_some());
}
