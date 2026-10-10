// A workflow or task whose handler could not call it is a compile error, not a
// failure when it runs.
use orcher_sdk::prelude::*;

#[workflow]
async fn two_inputs(_ctx: WorkflowContext, a: String, b: String) -> Result<String> {
    Ok(a + &b)
}

#[workflow]
async fn not_a_result(_ctx: WorkflowContext, input: String) -> String {
    input
}

#[task]
async fn task_two_inputs(_ctx: TaskContext, a: u32, b: u32) -> Result<u32> {
    Ok(a + b)
}

#[task]
async fn task_not_a_result(_ctx: TaskContext, input: u32) -> u32 {
    input
}

fn main() {}
