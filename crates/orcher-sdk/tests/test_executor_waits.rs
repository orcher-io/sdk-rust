//! `TestWorkflowExecutor` drives timers, events and child workflows, with virtual
//! time, from outside the crate as a user would.

use orcher_sdk::error::WorkflowError;
use orcher_sdk::prelude::*;
use orcher_sdk::testing::{MockRegistry, MockTaskBuilder, TestWorkflowExecutor};
use orcher_sdk::workflow::WorkflowExecution;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const HOUR: Duration = Duration::from_secs(3600);

fn execution() -> WorkflowExecution {
    WorkflowExecution::new(
        "wf-1".to_string(),
        "run-1".to_string(),
        "test".to_string(),
        "default".to_string(),
        "queue".to_string(),
    )
}

fn registry() -> Arc<RwLock<MockRegistry>> {
    Arc::new(RwLock::new(MockRegistry::new()))
}

#[tokio::test]
async fn a_long_sleep_returns_at_once_with_the_clock_moved_on() {
    let mocks = registry();
    MockTaskBuilder::new("send".to_string(), mocks.clone()).returns(true);

    let wf = |ctx: WorkflowContext, _: ()| async move {
        let before = ctx.time().elapsed();
        ctx.sleep(24 * HOUR).await?;
        let slept = ctx.time().elapsed() - before;
        let _: bool = ctx.execute_task("send", ()).await?;
        Ok(slept)
    };

    let started = std::time::Instant::now();
    let mut executor = TestWorkflowExecutor::new(mocks);
    let slept: Duration = executor
        .execute_workflow(wf, execution(), ())
        .await
        .unwrap();

    assert_eq!(slept, 24 * HOUR);
    assert_eq!(executor.elapsed(), 24 * HOUR);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "no real waiting"
    );
}

#[tokio::test]
async fn timers_fire_in_order_of_when_they_are_due() {
    let wf = |ctx: WorkflowContext, _: ()| async move {
        ctx.sleep(HOUR).await?;
        ctx.sleep(2 * HOUR).await?;
        Ok(ctx.time().elapsed())
    };

    let mut executor = TestWorkflowExecutor::new(registry());
    let at: Duration = executor
        .execute_workflow(wf, execution(), ())
        .await
        .unwrap();

    assert_eq!(at, 3 * HOUR);
    assert_eq!(executor.elapsed(), 3 * HOUR);
}

#[tokio::test]
async fn an_event_sent_at_the_start_is_received() {
    let wf = |ctx: WorkflowContext, _: ()| async move {
        let who: String = ctx.wait_for_event("approved_by").await?;
        Ok(who)
    };

    let mut executor = TestWorkflowExecutor::new(registry()).with_event("approved_by", "ada");
    let who: String = executor
        .execute_workflow(wf, execution(), ())
        .await
        .unwrap();

    assert_eq!(who, "ada");
    assert_eq!(executor.elapsed(), Duration::ZERO);
}

#[tokio::test]
async fn an_event_sent_later_is_received_when_its_time_comes() {
    let wf = |ctx: WorkflowContext, _: ()| async move {
        let n: u32 = ctx.wait_for_event("count").await?;
        Ok(n)
    };

    let mut executor =
        TestWorkflowExecutor::new(registry()).with_event_after(5 * HOUR, "count", 7u32);
    let n: u32 = executor
        .execute_workflow(wf, execution(), ())
        .await
        .unwrap();

    assert_eq!(n, 7);
    assert_eq!(executor.elapsed(), 5 * HOUR);
}

async fn wait_with_deadline(ctx: WorkflowContext, _: ()) -> Result<Option<bool>> {
    ctx.wait_for_event_with_timeout::<bool>("ok", 2 * HOUR)
        .await
}

#[tokio::test]
async fn an_event_before_the_deadline_wins() {
    let mut executor = TestWorkflowExecutor::new(registry()).with_event_after(HOUR, "ok", true);
    let got = executor
        .execute_workflow(wait_with_deadline, execution(), ())
        .await
        .unwrap();

    assert_eq!(got, Some(true));
    assert_eq!(executor.elapsed(), HOUR);
}

#[tokio::test]
async fn the_deadline_wins_over_a_later_event() {
    let mut executor = TestWorkflowExecutor::new(registry()).with_event_after(3 * HOUR, "ok", true);
    let got = executor
        .execute_workflow(wait_with_deadline, execution(), ())
        .await
        .unwrap();

    assert_eq!(got, None);
    assert_eq!(executor.elapsed(), 2 * HOUR);
}

#[tokio::test]
async fn a_child_workflow_is_answered_by_its_mock() {
    let children = registry();
    MockTaskBuilder::new("charge".to_string(), children.clone()).with_fn(|input| {
        let cents: u64 = serde_json::from_slice(&input).unwrap();
        Ok(serde_json::to_vec(&format!("charged {cents}")).unwrap())
    });

    let wf = |ctx: WorkflowContext, cents: u64| async move {
        let receipt: String = ctx.execute_child_workflow("charge", cents).await?;
        Ok(receipt)
    };

    let receipt: String = TestWorkflowExecutor::new(registry())
        .with_child_mocks(children)
        .execute_workflow(wf, execution(), 420u64)
        .await
        .unwrap();

    assert_eq!(receipt, "charged 420");
}

#[tokio::test]
async fn a_failed_child_reaches_the_parent_as_child_workflow_failed() {
    let children = registry();
    MockTaskBuilder::new("charge".to_string(), children.clone()).fails_with("card declined");

    let wf = |ctx: WorkflowContext, _: ()| async move {
        match ctx.execute_child_workflow::<_, String>("charge", ()).await {
            Err(Error::Workflow(WorkflowError::ChildWorkflowFailed { reason, .. })) => Ok(reason),
            // Anything else, a suspension included, is propagated.
            Err(e) => Err(e),
            Ok(v) => panic!("expected ChildWorkflowFailed, got the result {v}"),
        }
    };

    let reason: String = TestWorkflowExecutor::new(registry())
        .with_child_mocks(children)
        .execute_workflow(wf, execution(), ())
        .await
        .unwrap();

    assert_eq!(reason, "card declined");
}

#[tokio::test]
async fn a_child_with_no_mock_stops_the_run() {
    let wf = |ctx: WorkflowContext, _: ()| async move {
        ctx.execute_child_workflow::<_, String>("unmocked", ())
            .await
    };

    let result: Result<String> = TestWorkflowExecutor::new(registry())
        .execute_workflow(wf, execution(), ())
        .await;

    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("No mock registered for child workflow 'unmocked'"),
        "{message}"
    );
}

#[tokio::test]
async fn waiting_for_an_event_nobody_sends_stops_the_run() {
    let wf = |ctx: WorkflowContext, _: ()| async move {
        let n: u32 = ctx.wait_for_event("never").await?;
        Ok(n)
    };

    let result: Result<u32> = TestWorkflowExecutor::new(registry())
        .execute_workflow(wf, execution(), ())
        .await;

    let message = result.unwrap_err().to_string();
    assert!(message.contains("nothing left to deliver"), "{message}");
}
