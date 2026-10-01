//! Compile-and-behavior test for `#[task]` retry-policy code generation.
//!
//! The macro must register an `orcher::task::RetryPolicy` (with `Duration`
//! fields) in the process-global task retry registry, and
//! `task_retry_policy(name)` must return it. This is the only place the
//! `retry_policy(...)` and `retry = N` code paths are compiled, so it also
//! checks that the macro's output builds.
#![cfg(feature = "auto-register")]

use orcher::task::RetryPolicy;
use orcher::worker::registration::task_retry_policy;
use orcher::worker::Registry;
use orcher::{task, TaskContext};
use std::sync::Once;
use std::time::Duration;

/// The `#[task]` macro registers retry policies lazily: the generated registration
/// function only runs when a `Registry` collects from inventory. Trigger that once so
/// the process-global task retry registry is populated for every assertion below.
static COLLECTED: Once = Once::new();

fn ensure_registered() {
    COLLECTED.call_once(|| {
        Registry::new().collect_from_global_registry();
    });
}

#[task(retry_policy(
    max_attempts = 5,
    initial_interval = 2,
    max_interval = 30,
    backoff_coefficient = 3.0
))]
async fn task_with_full_policy(_ctx: TaskContext, input: String) -> orcher::Result<String> {
    Ok(input)
}

#[task(retry = 7)]
async fn task_with_retry_shortcut(_ctx: TaskContext, input: String) -> orcher::Result<String> {
    Ok(input)
}

#[task]
async fn task_without_policy(_ctx: TaskContext, input: String) -> orcher::Result<String> {
    Ok(input)
}

#[test]
fn full_retry_policy_registers_canonical_type() {
    ensure_registered();
    // The registry returns a task::RetryPolicy with Duration-typed intervals.
    let policy: RetryPolicy = task_retry_policy("task_with_full_policy")
        .expect("task_with_full_policy should have a registered retry policy");

    assert_eq!(policy.max_attempts, 5);
    assert_eq!(policy.initial_interval, Duration::from_secs(2));
    assert_eq!(policy.max_interval, Duration::from_secs(30));
    assert_eq!(policy.backoff_coefficient, 3.0);
}

#[test]
fn retry_shortcut_registers_policy_with_defaults() {
    ensure_registered();
    let policy = task_retry_policy("task_with_retry_shortcut")
        .expect("retry = N shortcut should register a policy");

    assert_eq!(policy.max_attempts, 7);
    // Shortcut fills interval/backoff with the standard exponential defaults.
    assert_eq!(policy.initial_interval, Duration::from_secs(1));
    assert_eq!(policy.max_interval, Duration::from_secs(60));
    assert_eq!(policy.backoff_coefficient, 2.0);
}

#[test]
fn task_without_policy_is_absent_from_registry() {
    ensure_registered();
    // No explicit policy => not registered; the scheduler falls back to the default.
    assert!(task_retry_policy("task_without_policy").is_none());
}
