//! A task or workflow declared with `name = "..."` is scheduled under the name the worker
//! registers it under, not under the function's name.
#![cfg(feature = "auto-register")]

use orcher_sdk::task::TaskReference;
use orcher_sdk::worker::Registry;
use orcher_sdk::{task, workflow, Result, TaskContext, WorkflowContext};

#[task(name = "declared-task-name")]
async fn renamed_task(_ctx: TaskContext, input: String) -> Result<String> {
    Ok(input)
}

#[task]
async fn unnamed_task(_ctx: TaskContext, input: String) -> Result<String> {
    Ok(input)
}

#[workflow(name = "declared-workflow-name")]
async fn renamed_workflow(_ctx: WorkflowContext, input: String) -> Result<String> {
    Ok(input)
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.collect_from_global_registry();
    registry
}

#[test]
fn a_task_reference_schedules_the_declared_name_the_worker_registers() {
    let registry = registry();

    assert_eq!(renamed_task.task_name(), "declared-task-name");
    assert!(registry.tasks.contains_key(renamed_task.task_name()));
    assert!(!registry.tasks.contains_key("renamed_task"));
}

#[test]
fn a_task_without_a_name_is_scheduled_under_its_function_name() {
    let registry = registry();

    assert_eq!(unnamed_task.task_name(), "unnamed_task");
    assert!(registry.tasks.contains_key(unnamed_task.task_name()));
}

#[test]
fn a_workflow_reports_the_declared_name_the_worker_registers() {
    let registry = registry();

    assert_eq!(Renamed_workflowWorkflow::name(), "declared-workflow-name");
    assert!(registry
        .workflows
        .contains_key(Renamed_workflowWorkflow::name()));
}
