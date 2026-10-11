//! A `#[task]` body, kept as `<name>_impl`, can be called directly in a test
//! with a `TestTaskContext`, from outside the crate as a user would.

use orcher_sdk::prelude::*;
use orcher_sdk::testing::TestTaskContext;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Progress {
    sent: u32,
}

#[task(retry = 3)]
async fn send_emails(ctx: TaskContext, count: u32) -> Result<String> {
    for sent in 1..=count {
        if ctx.is_cancelled() {
            return Ok(format!("stopped after {}", sent - 1));
        }
        ctx.heartbeat_with_details(Some(Progress { sent })).await?;
    }
    Ok(format!("attempt {} sent {count}", ctx.attempt()))
}

#[tokio::test]
async fn a_task_runs_with_a_test_context() {
    let (ctx, probe) = TestTaskContext::new().attempt(2).build();

    let out = send_emails_impl(ctx, 3).await.unwrap();

    assert_eq!(out, "attempt 2 sent 3");
    assert_eq!(
        probe.heartbeat_details::<Progress>().unwrap(),
        vec![
            Progress { sent: 1 },
            Progress { sent: 2 },
            Progress { sent: 3 }
        ]
    );
}

#[tokio::test]
async fn a_cancelled_task_sees_the_cancellation() {
    let (ctx, probe) = TestTaskContext::new().build();
    probe.cancel();

    let out = send_emails_impl(ctx, 3).await.unwrap();

    assert_eq!(out, "stopped after 0");
    assert_eq!(probe.heartbeat_count(), 0);
}
