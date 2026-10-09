//! Every `#[non_exhaustive]` type a user builds can still be built from
//! outside this crate, which is the only place the attribute has an effect.

use std::time::Duration;

use orcher_sdk::client::{
    CancelOptions, ClientConfig, ClientTlsConfig, EventOptions, QueryOptions, QueryRejectCondition,
    StartWorkflowOptions, WorkflowIdReusePolicy,
};
use orcher_sdk::error::{ClientError, TaskError, WorkerError, WorkflowError};
use orcher_sdk::task::{JitterStrategy, RetryPolicy, TaskExecutionOptions};
use orcher_sdk::workflow::{ChildWorkflowOptions, OrphanPolicy, RestartFreshOptions, TaskOptions};
use orcher_sdk::{Error, ErrorCode};

#[test]
fn client_options_build_with_their_methods() {
    let config = ClientConfig::new("http://orcher:50051")
        .with_namespace("orders")
        .with_timeout(Duration::from_secs(5))
        .with_api_key("key")
        .with_organization_id("org")
        .with_tls(
            ClientTlsConfig::new()
                .with_ca_cert(b"ca".to_vec())
                .with_client_identity(b"cert".to_vec(), b"key".to_vec())
                .with_domain_name("orcher.local"),
        );
    assert_eq!(config.server_url, "http://orcher:50051");
    assert_eq!(config.namespace, "orders");
    assert_eq!(config.api_key.as_deref(), Some("key"));
    assert!(config.tls.is_some());

    let start = StartWorkflowOptions::new("orders")
        .with_workflow_id("order-1")
        .with_namespace("ns")
        .with_workflow_execution_timeout(Duration::from_secs(60))
        .with_retry_policy(RetryPolicy::default())
        .with_id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate);
    assert_eq!(start.task_queue, "orders");
    assert_eq!(start.workflow_id.as_deref(), Some("order-1"));
    assert_eq!(
        start.id_reuse_policy,
        Some(WorkflowIdReusePolicy::RejectDuplicate)
    );

    let query = QueryOptions::default()
        .with_timeout(Duration::from_secs(1))
        .with_reject_condition(QueryRejectCondition::NotOpen);
    assert_eq!(query.reject_condition, Some(QueryRejectCondition::NotOpen));
    let event = EventOptions::default().with_timeout(Duration::from_secs(2));
    assert_eq!(event.timeout, Some(Duration::from_secs(2)));
    let cancel = CancelOptions::default().with_cleanup_timeout(Duration::from_secs(3));
    assert_eq!(cancel.cleanup_timeout, Some(Duration::from_secs(3)));
}

#[test]
fn workflow_and_task_options_build_with_their_methods() {
    let policy = RetryPolicy::default()
        .with_max_attempts(5)
        .with_initial_interval(Duration::from_secs(2))
        .with_max_interval(Duration::from_secs(30))
        .with_backoff_coefficient(1.5)
        .with_jitter(JitterStrategy::Full)
        .with_non_retryable_errors(["CardDeclined"]);
    assert_eq!(policy.max_attempts, 5);
    assert_eq!(
        policy.non_retryable_errors,
        vec!["CardDeclined".to_string()]
    );

    let task = TaskOptions::default()
        .with_timeout(Duration::from_secs(10))
        .with_task_id("t-1")
        .with_retry_policy(policy)
        .with_heartbeat_timeout(Duration::from_secs(3))
        .with_queue_timeout(Duration::from_secs(4));
    assert_eq!(task.queue_timeout, Some(Duration::from_secs(4)));

    let child = ChildWorkflowOptions::new()
        .workflow_id("child")
        .orphan_policy(OrphanPolicy::Abandon);
    assert_eq!(child.orphan_policy, OrphanPolicy::Abandon);
    let restart = RestartFreshOptions::new().task_queue("q");
    assert_eq!(restart.task_queue.as_deref(), Some("q"));

    let exec = TaskExecutionOptions::default()
        .with_max_attempts(2)
        .with_retry_backoff_coefficient(3.0);
    assert_eq!(exec.max_attempts, Some(2));
}

#[test]
fn errors_build_with_constructors_and_match_with_rest_patterns() {
    let err: Error = ClientError::workflow_already_exists_with_run("order-1", "run-1").into();
    assert_eq!(err.code(), ErrorCode::WorkflowAlreadyExists);
    match err {
        Error::Client(ClientError::WorkflowAlreadyExists { run_id, .. }) => {
            assert_eq!(run_id.as_deref(), Some("run-1"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let built: Vec<Error> = vec![
        WorkflowError::version_mismatch("1", "2").into(),
        WorkflowError::suspended("waiting", vec!["t-1".into()]).into(),
        TaskError::heartbeat_timeout(Duration::from_secs(1)).into(),
        TaskError::already_completed("t-1").into(),
        TaskError::application("CardDeclined", "declined").into(),
        WorkerError::task_queue_not_found("q").into(),
    ];
    assert_eq!(built.len(), 6);
}
