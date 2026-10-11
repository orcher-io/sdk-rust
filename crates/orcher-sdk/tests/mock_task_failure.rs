//! A mocked task failure reaches the workflow as `TaskFailed`, as a task that
//! fails for good does on a real engine, rather than aborting the test run.

use orcher_sdk::error::WorkflowError;
use orcher_sdk::prelude::*;
use orcher_sdk::testing::{MockRegistry, MockTaskBuilder, TestWorkflowExecutor};
use orcher_sdk::workflow::{Saga, WorkflowExecution};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

fn execution() -> WorkflowExecution {
    WorkflowExecution::new(
        "wf-1".to_string(),
        "run-1".to_string(),
        "checkout".to_string(),
        "default".to_string(),
        "orders".to_string(),
    )
}

fn mock(mocks: &Arc<RwLock<MockRegistry>>, task: &str) -> MockTaskBuilder {
    MockTaskBuilder::new(task.to_string(), mocks.clone())
}

#[tokio::test]
async fn a_workflow_catches_a_mocked_task_failure() {
    let mocks = Arc::new(RwLock::new(MockRegistry::new()));
    mock(&mocks, "charge_card").fails_with("card declined");
    mock(&mocks, "charge_paypal").returns("pp_1");

    let checkout = |ctx: WorkflowContext, order: String| async move {
        match ctx
            .execute_task::<_, _, String>("charge_card", order.clone())
            .await
        {
            Ok(id) => Ok(id),
            Err(Error::Workflow(WorkflowError::TaskFailed { reason, .. })) => {
                assert_eq!(reason, "card declined");
                ctx.execute_task::<_, _, String>("charge_paypal", order)
                    .await
            }
            Err(e) => Err(e),
        }
    };

    let charged: String = TestWorkflowExecutor::new(mocks)
        .execute_workflow(checkout, execution(), "order-1".to_string())
        .await
        .unwrap();

    assert_eq!(charged, "pp_1");
}

#[tokio::test]
async fn a_saga_compensates_when_a_mocked_step_fails() {
    let mocks = Arc::new(RwLock::new(MockRegistry::new()));
    mock(&mocks, "reserve").returns("r_1");
    let released = Arc::new(AtomicUsize::new(0));
    let counter = released.clone();
    mock(&mocks, "release").with_fn(move |_input| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::to_vec(&true).unwrap())
    });
    mock(&mocks, "charge").fails_with("insufficient funds");

    let checkout = |ctx: WorkflowContext, order: String| async move {
        let mut saga = Saga::new();
        let _: String = saga
            .add_step(&ctx, "reserve", order.clone())
            .with_compensation("release")
            .execute()
            .await?;
        let _: String = saga.add_step(&ctx, "charge", order).execute().await?;
        saga.commit();
        Ok("done".to_string())
    };

    let result: Result<String> = TestWorkflowExecutor::new(mocks.clone())
        .execute_workflow(checkout, execution(), "order-1".to_string())
        .await;

    // The workflow fails with the step's failure, after compensating.
    assert!(matches!(
        result,
        Err(Error::Workflow(WorkflowError::TaskFailed { ref reason, .. })) if reason == "insufficient funds"
    ));
    // Replays re-run the workflow, but the compensation is a task, so it ran once.
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_missing_mock_still_stops_the_run() {
    let mocks = Arc::new(RwLock::new(MockRegistry::new()));

    let wf = |ctx: WorkflowContext, _: String| async move {
        ctx.execute_task::<_, _, String>("unmocked", ()).await
    };

    let result: Result<String> = TestWorkflowExecutor::new(mocks)
        .execute_workflow(wf, execution(), String::new())
        .await;

    match result {
        Err(e) => assert!(e.to_string().contains("No mock registered"), "{e}"),
        Ok(v) => panic!("expected the run to stop, got {v}"),
    }
}
