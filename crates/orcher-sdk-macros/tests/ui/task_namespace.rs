// A task belongs to no namespace of its own, so `namespace` is rejected.
use orcher_sdk::prelude::*;

#[task(namespace = "production")]
async fn charge(_ctx: TaskContext, input: String) -> Result<String> {
    Ok(input)
}

pub struct Shipping;

#[tasks(namespace = "production")]
impl Shipping {
    #[task]
    async fn ship(_ctx: TaskContext, input: String) -> Result<String> {
        Ok(input)
    }
}

fn main() {}
