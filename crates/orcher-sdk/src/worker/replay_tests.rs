//! Replay determinism, driven through the runtime as a worker drives it.
//!
//! A small in-memory engine journals what each activation asks for, resolves
//! it, and hands the grown journal to the next activation, the way the engine
//! does. Every activation re-runs the workflow from the start against that
//! journal, so a step id or a clock reading that depends on how far the run
//! had got shows up as a difference between activations.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_proto::orcher::v1::journal_entry::Attributes;
use orcher_proto::orcher::v1::{
    ChildWorkflowExecutionCompletedEventAttributes, ChildWorkflowExecutionStartedEventAttributes,
    EntryType, JournalEntry, StepCompletedEventAttributes, TaskScheduledEventAttributes,
    TimerFiredEventAttributes, TimerStartedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
};
use orcher_sdk_core::bridge::{Command, ExecutionErrorType, ExecutionResult};
use orcher_sdk_core::poller::WorkflowExecutionTask;
use orcher_sdk_core::Replayer;

use crate::payload::Payload;
use crate::worker::registration::WorkflowHandlerFn;
use crate::worker::runtime::{ExecutionRuntime, RuntimeConfig};
use crate::workflow::WorkflowContext;

/// Where the fake engine's clock starts: 2020-09-13, nowhere near the wall
/// clock, so a reading taken from the wall clock cannot pass for it.
const ENGINE_EPOCH_MS: i64 = 1_600_000_000_000;
/// How far the fake engine's clock moves between two journal entries.
const ENTRY_GAP_MS: i64 = 7_000;

/// An engine that journals and resolves whatever an activation asks for.
struct FakeEngine {
    journal: Vec<JournalEntry>,
    clock_ms: i64,
}

impl FakeEngine {
    fn new() -> Self {
        let mut engine = Self {
            journal: Vec::new(),
            clock_ms: ENGINE_EPOCH_MS,
        };
        engine.append(
            EntryType::WorkflowExecutionStarted,
            Attributes::WorkflowExecutionStarted(WorkflowExecutionStartedEventAttributes {
                workflow_type: "under_test".into(),
                ..Default::default()
            }),
        );
        engine
    }

    fn append(&mut self, entry_type: EntryType, attributes: Attributes) {
        let at = self.clock_ms;
        self.clock_ms += ENTRY_GAP_MS;
        self.journal.push(JournalEntry {
            entry_id: self.journal.len() as i64 + 1,
            timestamp: Some(orcher_proto::prost_types::Timestamp {
                seconds: at / 1000,
                nanos: ((at % 1000) * 1_000_000) as i32,
            }),
            entry_type: entry_type as i32,
            attributes: Some(attributes),
            ..Default::default()
        });
    }

    /// When the entry at `index` was recorded, in whole seconds.
    fn entry_secs(&self, index: usize) -> i64 {
        self.journal[index].timestamp.as_ref().unwrap().seconds
    }

    fn activation(&self) -> WorkflowExecutionTask {
        WorkflowExecutionTask {
            execution: orcher_sdk_core::WorkflowExecution::new("wf-replay", "run-replay"),
            workflow_type: "under_test".into(),
            task_queue: "replay".into(),
            input: serde_json::json!(null),
            journal: self.journal.clone(),
            task_token: Vec::new(),
            started_event_id: 1,
            previous_started_event_id: 0,
            attempt: 1,
            stream_entry_id: None,
            queries: Vec::new(),
            updates: Vec::new(),
        }
    }

    /// Journal and resolve what an activation asked for. A task's result is
    /// its own step id and a child's is its workflow id, so a workflow handed
    /// another step's result says so in its output.
    fn apply(&mut self, commands: &[Command]) {
        for command in commands {
            match command {
                Command::ScheduleTask(cmd) => {
                    self.append(
                        EntryType::TaskScheduled,
                        Attributes::TaskScheduled(TaskScheduledEventAttributes {
                            task_id: cmd.task_id.clone(),
                            task_type: cmd.task_type.clone(),
                            ..Default::default()
                        }),
                    );
                    self.append(
                        EntryType::StepCompleted,
                        Attributes::StepCompleted(StepCompletedEventAttributes {
                            step_name: cmd.task_id.clone(),
                            result: serde_json::to_vec(&cmd.task_id).unwrap(),
                            ..Default::default()
                        }),
                    );
                }
                Command::StartTimer(cmd) => {
                    self.append(
                        EntryType::TimerStarted,
                        Attributes::TimerStarted(TimerStartedEventAttributes {
                            timer_id: cmd.timer_id.clone(),
                            ..Default::default()
                        }),
                    );
                    self.append(
                        EntryType::TimerFired,
                        Attributes::TimerFired(TimerFiredEventAttributes {
                            timer_id: cmd.timer_id.clone(),
                            ..Default::default()
                        }),
                    );
                }
                Command::StartChildWorkflow(cmd) => {
                    self.append(
                        EntryType::ChildWorkflowExecutionStarted,
                        Attributes::ChildWorkflowExecutionStarted(
                            ChildWorkflowExecutionStartedEventAttributes {
                                workflow_id: cmd.workflow_id.clone(),
                                workflow_type: cmd.workflow_type.clone(),
                                ..Default::default()
                            },
                        ),
                    );
                    self.append(
                        EntryType::ChildWorkflowExecutionCompleted,
                        Attributes::ChildWorkflowExecutionCompleted(
                            ChildWorkflowExecutionCompletedEventAttributes {
                                workflow_id: cmd.workflow_id.clone(),
                                result: serde_json::to_vec(&cmd.workflow_id).unwrap(),
                                ..Default::default()
                            },
                        ),
                    );
                }
                Command::RecordStepResult(cmd) => {
                    self.append(
                        EntryType::StepCompleted,
                        Attributes::StepCompleted(StepCompletedEventAttributes {
                            step_name: cmd.step_name.clone(),
                            step_type: cmd.step_type,
                            result: cmd.result.clone(),
                            ..Default::default()
                        }),
                    );
                }
                _ => {}
            }
        }
    }
}

/// A step an activation issued: its kind, its id, and the work it names.
type Issued = (&'static str, String, String);

fn issued_steps(commands: &[Command]) -> Vec<Issued> {
    commands
        .iter()
        .filter_map(|command| match command {
            Command::ScheduleTask(c) => Some(("task", c.task_id.clone(), c.task_type.clone())),
            Command::StartTimer(c) => Some(("timer", c.timer_id.clone(), String::new())),
            Command::StartChildWorkflow(c) => {
                Some(("child", c.workflow_id.clone(), c.workflow_type.clone()))
            }
            Command::RecordStepResult(c) => Some(("closure", c.step_name.clone(), String::new())),
            _ => None,
        })
        .collect()
}

fn completion(result: &ExecutionResult) -> Option<serde_json::Value> {
    result.commands.iter().find_map(|command| match command {
        Command::CompleteWorkflow(c) => Some(serde_json::from_slice(&c.result.data).unwrap()),
        _ => None,
    })
}

/// Run one activation and hold it to what the worker's driver checks: no
/// failure, no step id the journal recorded reissued for other work, and no
/// recorded step left unreached by an activation that issues new work or ends
/// the workflow.
async fn activate(runtime: &ExecutionRuntime, engine: &FakeEngine) -> ExecutionResult {
    let result = runtime.execute_workflow(engine.activation()).await;
    if let Some(error) = &result.error {
        assert_ne!(
            error.error_type,
            ExecutionErrorType::NonDeterminism,
            "the activation failed as non-deterministic: {}",
            error.message
        );
        panic!("the activation failed: {}", error.message);
    }
    assert!(result.successful);
    assert!(
        result.reached_steps.is_some(),
        "the activation does not report the steps it reached"
    );
    let violations = Replayer::check_activation(&engine.journal, &result);
    assert!(
        violations.is_empty(),
        "the activation does not replay its journal: {violations:?}"
    );
    result
}

/// Drive a workflow to completion, one activation per journal state. Returns
/// every step each activation issued, and the output.
async fn run_to_completion(
    runtime: &ExecutionRuntime,
    engine: &mut FakeEngine,
) -> (Vec<Issued>, serde_json::Value) {
    let mut issued = Vec::new();
    for _ in 0..16 {
        let result = activate(runtime, engine).await;
        issued.extend(issued_steps(&result.commands));
        // Journaled whether or not the workflow finished: a completion
        // carries the results of the closures it ran.
        engine.apply(&result.commands);
        if let Some(output) = completion(&result) {
            return (issued, output);
        }
    }
    panic!("the workflow did not complete; it issued {issued:?}");
}

async fn runtime_with(handler: WorkflowHandlerFn) -> ExecutionRuntime {
    let runtime = ExecutionRuntime::new(RuntimeConfig::default());
    runtime
        .register_workflow_handler("under_test", handler)
        .await;
    runtime
}

fn output_of<T: serde::Serialize>(value: T) -> crate::Result<Payload> {
    Ok(Payload::new_data(serde_json::to_vec(&value).unwrap()))
}

#[tokio::test]
async fn a_replayed_workflow_issues_every_step_under_the_id_it_first_had() {
    let handler: WorkflowHandlerFn = Arc::new(|ctx: WorkflowContext, _input| {
        Box::pin(async move {
            // A task and a timer in flight together, then a child, a closure
            // and another task, each after the one before it has finished.
            let (a, slept) = futures::join!(
                ctx.execute_task_by_name::<u32, String>("step_a", 1),
                ctx.sleep(Duration::from_secs(5)),
            );
            let a = a?;
            slept?;
            let child: String = ctx.execute_child_workflow("child_wf", ()).await?;
            let closure: String = ctx
                .execute("closure_c", || async { Ok("closure ran".to_string()) })
                .await?;
            let b: String = ctx.execute_task_by_name("step_b", 2).await?;
            output_of(vec![a, child, closure, b])
        })
    });
    let runtime = runtime_with(handler).await;
    let mut engine = FakeEngine::new();

    let (issued, output) = run_to_completion(&runtime, &mut engine).await;

    // One counter for every kind of step, each id drawn once, whatever the
    // activation: the first run and every replay agree on all of them.
    assert_eq!(
        issued,
        vec![
            ("task", "step_a_0".to_string(), "step_a".to_string()),
            ("timer", "timer_1".to_string(), String::new()),
            ("child", "child_2".to_string(), "child_wf".to_string()),
            ("closure", "closure_c_3".to_string(), String::new()),
            ("task", "step_b_4".to_string(), "step_b".to_string()),
        ],
        "each step is issued once, under one id"
    );
    // Each step got its own result back.
    assert_eq!(
        output,
        serde_json::json!(["step_a_0", "child_2", "closure ran", "step_b_4"])
    );

    // Replayed from the finished journal, as a worker that never saw the run
    // does: nothing is issued again, and the result is the same.
    let replay = activate(&runtime, &engine).await;
    assert_eq!(issued_steps(&replay.commands), Vec::<Issued>::new());
    assert_eq!(completion(&replay), Some(output));
}

#[tokio::test]
async fn workflow_time_comes_from_the_journal_and_is_the_same_on_every_replay() {
    let readings: Arc<Mutex<Vec<(&'static str, i64)>>> = Arc::default();
    let seen = Arc::clone(&readings);
    let handler: WorkflowHandlerFn = Arc::new(move |ctx: WorkflowContext, _input| {
        let seen = Arc::clone(&seen);
        Box::pin(async move {
            let read = |label: &'static str, value: i64| {
                seen.lock().unwrap().push((label, value));
                value
            };
            let started = read("started", ctx.time().now());
            ctx.execute_task_by_name::<u32, String>("step_a", 1).await?;
            let after_task = read("after task", ctx.time().now());
            ctx.sleep(Duration::from_secs(60)).await?;
            let after_timer = read("after timer", ctx.time().now());
            ctx.execute("closure_c", || async { Ok(1u32) }).await?;
            let after_closure = read("after closure", ctx.time().now());
            let elapsed = read("elapsed", ctx.time().elapsed_secs() as i64);
            output_of(vec![
                started,
                after_task,
                after_timer,
                after_closure,
                elapsed,
            ])
        })
    });
    let runtime = runtime_with(handler).await;
    let mut engine = FakeEngine::new();

    let (_, output) = run_to_completion(&runtime, &mut engine).await;

    // Journal entries: 0 started, 1 task scheduled, 2 task completed,
    // 3 timer started, 4 timer fired, 5 closure recorded.
    let started = engine.entry_secs(0);
    let task_completed = engine.entry_secs(2);
    let timer_fired = engine.entry_secs(4);
    // A closure runs inline, so it moves nothing: the activation that ran it
    // and the replays that read its journaled result must agree.
    let expected = serde_json::json!([
        started,
        task_completed,
        timer_fired,
        timer_fired,
        timer_fired - started
    ]);
    assert_eq!(output, expected, "time is read from the journal");

    // Every activation that got as far as a reading took the same one.
    let readings = readings.lock().unwrap().clone();
    assert!(readings.len() > 5, "the workflow ran more than once");
    for (label, value) in &readings {
        let first = readings.iter().find(|(l, _)| l == label).map(|(_, v)| v);
        assert_eq!(Some(value), first, "{label} differs between activations");
    }

    // Replayed later against the same journal, it reads the same times: the
    // wall clock has moved on and the workflow's clock has not.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let replay = activate(&runtime, &engine).await;
    assert_eq!(completion(&replay), Some(expected));
}

#[tokio::test]
async fn a_closure_before_a_suspension_runs_once_and_is_journaled_before_it() {
    let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let handler: WorkflowHandlerFn = Arc::new(move |ctx: WorkflowContext, _input| {
        let counted = Arc::clone(&counted);
        Box::pin(async move {
            let value: u32 = ctx
                .execute("closure_c", move || async move {
                    Ok(counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as u32 + 7)
                })
                .await?;
            ctx.sleep(Duration::from_secs(5)).await?;
            output_of(value)
        })
    });
    let runtime = runtime_with(handler).await;
    let mut engine = FakeEngine::new();

    let (issued, output) = run_to_completion(&runtime, &mut engine).await;

    assert_eq!(
        runs.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the closure ran once"
    );
    assert_eq!(
        output,
        serde_json::json!(7),
        "its first result is the one kept"
    );
    // Reported with the activation that ran it, ahead of the timer it suspended on.
    assert_eq!(
        issued,
        vec![
            ("closure", "closure_c_0".to_string(), String::new()),
            ("timer", "timer_1".to_string(), String::new()),
        ]
    );

    // A worker replaying the finished journal reads the result back.
    let replay = activate(&runtime, &engine).await;
    assert_eq!(issued_steps(&replay.commands), Vec::<Issued>::new());
    assert_eq!(completion(&replay), Some(output));
    assert_eq!(
        runs.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "not run on replay"
    );
}

/// A status watch: wait for a `status` event with a deadline, apply what came
/// (or nothing) with a task, and escalate after `quiet_cap` rounds in a row
/// with no event. The fake engine fires every timer at once.
fn status_watch(quiet_cap: u32) -> WorkflowHandlerFn {
    Arc::new(move |ctx: WorkflowContext, _input| {
        Box::pin(async move {
            let mut quiet = 0;
            for round in 1u32.. {
                let ev: Option<String> = ctx
                    .wait_for_event_with_timeout("status", Duration::from_secs(4))
                    .await?;
                let _: String = ctx.execute_task_by_name("applyStatus", round).await?;
                quiet = if ev.is_some() { 0 } else { quiet + 1 };
                if quiet >= quiet_cap {
                    let _: String = ctx.execute_task_by_name("escalate", round).await?;
                    return output_of(format!("escalated at round {round}"));
                }
            }
            unreachable!()
        })
    })
}

/// The code changed to give up after one quiet round, replaying a run the old
/// code recorded three rounds of: it takes round one from the journal and
/// schedules the escalation where the journal holds round two. The activation
/// reports what it reached, and sdk-core refuses it; the old code goes on.
#[tokio::test]
async fn a_loop_cut_short_is_refused_where_the_journal_goes_on() {
    let old = runtime_with(status_watch(5)).await;
    let mut engine = FakeEngine::new();
    for _ in 0..6 {
        let result = activate(&old, &engine).await;
        engine.apply(&result.commands);
    }

    let new = runtime_with(status_watch(1)).await;
    let result = new.execute_workflow(engine.activation()).await;
    assert_eq!(
        result.reached_steps.as_deref(),
        Some(
            &[
                "event_timeout_status_1".to_string(),
                "applyStatus_0".to_string(),
                "escalate_1".to_string(),
            ][..]
        )
    );
    let violations = Replayer::check_activation(&engine.journal, &result);
    let [violation] = violations.as_slice() else {
        panic!("expected one violation, got {violations:?}");
    };
    assert_eq!(violation.step_id.as_deref(), Some("event_timeout_status_2"));
    assert_eq!(
        violation.actual.as_deref(),
        Some(r#"task "escalate_1" of type "escalate""#)
    );

    let result = activate(&old, &engine).await;
    assert_eq!(
        issued_steps(&result.commands),
        vec![("timer", "event_timeout_status_4".to_string(), String::new())]
    );
}
