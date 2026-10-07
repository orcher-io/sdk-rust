// `timeout`, `version` and `namespace` on #[workflow] are accepted but have no
// effect, so each one must produce a deprecation warning naming what to use
// instead. Denying the lint turns those warnings into the errors checked here.
#![deny(deprecated)]

use orcher_sdk::prelude::*;

#[workflow(
    name = "ineffective",
    timeout = 600,
    version = "2.0.0",
    namespace = "production"
)]
async fn ineffective(_ctx: WorkflowContext, input: String) -> Result<String> {
    Ok(input)
}

fn main() {}
