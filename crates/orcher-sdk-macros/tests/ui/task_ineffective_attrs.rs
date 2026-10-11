// Attributes on #[task] and #[tasks] that are accepted but have no effect each
// produce a deprecation warning naming what to do instead. Denying the lint turns
// those warnings into the errors checked here.
#![deny(deprecated)]

use orcher_sdk::prelude::*;

#[task(task_queue = "billing", priority = 5)]
async fn charge(_ctx: TaskContext, input: String) -> Result<String> {
    Ok(input)
}

pub struct Shipping;

#[tasks(max_concurrent = 2)]
impl Shipping {
    #[task(description = "Ships an order")]
    async fn ship(_ctx: TaskContext, input: String) -> Result<String> {
        Ok(input)
    }
}

fn main() {}
