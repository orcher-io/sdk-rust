//! [`TestEnv`], an in-memory environment for running workflows in unit tests without an
//! Orcher server.

use crate::debugging::{CommandInspector, ExecutionTracer, StateSnapshot};
use crate::error::{Error, Result};
use crate::testing::mocks::MockRegistry;
use crate::testing::time::MockClock;
use crate::workflow::{WorkflowContext, WorkflowExecution};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// In-memory environment for running workflows in tests.
///
/// Register workflows, run them with [`execute_workflow`](Self::execute_workflow), and
/// inspect each run's recorded status afterwards.
///
/// `execute_workflow` does not serve tasks from the mocks registered with
/// [`mock_task`](Self::mock_task): a workflow that calls `ctx.execute_task` suspends and the
/// run returns an error. It also does not record state, executed tasks or task calls from
/// the run, so the methods that read them find nothing for it. Use
/// [`TestWorkflowExecutor`](crate::testing::TestWorkflowExecutor) to run a workflow against
/// mocked tasks.
///
/// The synchronous inspection methods take a blocking lock and panic if called from inside
/// an async runtime.
///
/// # Example
///
/// ```rust
/// use orcher::prelude::*;
/// use orcher::testing::TestEnv;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<()> {
/// let mut env = TestEnv::new();
/// env.register_workflow("double", |n: i64| async move { Ok(n * 2) });
///
/// let result: i64 = env.execute_workflow("double", 21).await?;
/// assert_eq!(result, 42);
/// # Ok(())
/// # }
/// ```
pub struct TestEnv {
    mocks: Arc<RwLock<MockRegistry>>,

    /// Execution traces keyed by workflow_id
    traces: Arc<Mutex<HashMap<String, ExecutionTrace>>>,

    /// Workflows without a context, keyed by workflow type
    workflows: Arc<RwLock<HashMap<String, WorkflowHandler>>>,

    /// Workflows that take a [`WorkflowContext`], keyed by workflow type
    context_workflows: Arc<RwLock<HashMap<String, ContextWorkflowHandler>>>,

    /// Task calls keyed by task type
    task_calls: Arc<Mutex<HashMap<String, Vec<TaskCall>>>>,

    namespace: String,

    task_queue: String,

    tracer: Option<ExecutionTracer>,

    capture_snapshots: bool,

    /// State snapshots keyed by workflow_id
    snapshots: Arc<Mutex<HashMap<String, Vec<StateSnapshot>>>>,

    mock_clock: Option<MockClock>,
}

/// What the test environment recorded about one workflow run.
#[derive(Debug, Clone)]
pub struct ExecutionTrace {
    /// Workflow ID
    #[allow(dead_code)] // Read by users, not by the crate
    pub workflow_id: String,

    /// Workflow type
    #[allow(dead_code)] // Read by users, not by the crate
    pub workflow_type: String,

    /// Execution status
    pub status: ExecutionStatus,

    /// Workflow state as JSON bytes, keyed by state key
    pub state: HashMap<String, Vec<u8>>,

    /// Types of the tasks executed, in order
    pub tasks_executed: Vec<String>,

    /// Commands generated
    #[allow(dead_code)] // Read by users, not by the crate
    pub commands: Vec<String>,

    /// Events received
    #[allow(dead_code)] // Read by users, not by the crate
    pub events: Vec<String>,

    /// Queries handled
    #[allow(dead_code)] // Read by users, not by the crate
    pub queries: Vec<String>,
}

/// Workflow execution status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStatus {
    /// Workflow is running
    Running,

    /// Workflow completed successfully
    Completed,

    /// Workflow failed
    Failed(String),

    /// Workflow was canceled
    Cancelled,
}

/// Record of a task call
#[derive(Debug, Clone)]
pub struct TaskCall {
    /// Task type name
    pub task_type: String,

    /// Task input as JSON bytes
    pub input: Vec<u8>,

    /// When the call was recorded
    pub timestamp: std::time::Instant,
}

/// A registered workflow without a context, erased to JSON bytes in and out.
type WorkflowHandler =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>> + Send + Sync>;

/// A registered workflow that takes a [`WorkflowContext`], erased to JSON bytes in and out.
type ContextWorkflowHandler = Arc<
    dyn Fn(WorkflowContext, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

impl TestEnv {
    /// Create a test environment with namespace `test` and task queue `test-queue`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher::testing::TestEnv;
    ///
    /// let env = TestEnv::new();
    /// ```
    pub fn new() -> Self {
        Self {
            mocks: Arc::new(RwLock::new(MockRegistry::new())),
            traces: Arc::new(Mutex::new(HashMap::new())),
            workflows: Arc::new(RwLock::new(HashMap::new())),
            context_workflows: Arc::new(RwLock::new(HashMap::new())),
            task_calls: Arc::new(Mutex::new(HashMap::new())),
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
            tracer: None,
            capture_snapshots: false,
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            mock_clock: None,
        }
    }

    /// Set the default namespace for workflows.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the default task queue for workflows.
    pub fn with_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = task_queue.into();
        self
    }

    /// Record each run's completion or failure in the given tracer.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher::testing::TestEnv;
    /// use orcher::debugging::ExecutionTracer;
    ///
    /// let tracer = ExecutionTracer::new().enable();
    /// let env = TestEnv::new().with_tracer(tracer);
    /// ```
    pub fn with_tracer(mut self, tracer: ExecutionTracer) -> Self {
        self.tracer = Some(tracer);
        self
    }

    /// Capture state snapshots during runs.
    ///
    /// When enabled, each run captures one snapshot, labeled `initial`, before the
    /// workflow starts. Read them with [`get_snapshots`](Self::get_snapshots).
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher::testing::TestEnv;
    ///
    /// let env = TestEnv::new().with_snapshots(true);
    /// ```
    pub fn with_snapshots(mut self, enabled: bool) -> Self {
        self.capture_snapshots = enabled;
        self
    }

    /// Attach a [`MockClock`] that tests advance by hand.
    ///
    /// The environment holds the clock so a test can reach it through
    /// [`clock`](Self::clock) and [`advance_time`](Self::advance_time). Workflows run by
    /// [`execute_workflow`](Self::execute_workflow) do not read it.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher::testing::{TestEnv, MockClock};
    /// use std::time::Duration;
    ///
    /// let clock = MockClock::new();
    /// let env = TestEnv::new().with_mock_clock(clock.clone());
    ///
    /// clock.advance(Duration::from_secs(10));
    /// assert_eq!(env.clock().unwrap().elapsed_ms(), 10000);
    /// ```
    pub fn with_mock_clock(mut self, clock: MockClock) -> Self {
        self.mock_clock = Some(clock);
        self
    }

    /// The mock clock, if one is attached.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher::testing::{TestEnv, MockClock};
    /// use std::time::Duration;
    ///
    /// let clock = MockClock::new();
    /// let env = TestEnv::new().with_mock_clock(clock);
    ///
    /// let c = env.clock().unwrap();
    /// c.advance(Duration::from_secs(5));
    /// assert_eq!(c.elapsed_ms(), 5000);
    /// ```
    pub fn clock(&self) -> Option<&MockClock> {
        self.mock_clock.as_ref()
    }

    /// Advance the mock clock by `duration`.
    ///
    /// # Panics
    ///
    /// Panics if no clock was attached with [`with_mock_clock`](Self::with_mock_clock).
    pub fn advance_time(&self, duration: std::time::Duration) {
        self.mock_clock
            .as_ref()
            .expect("advance_time() requires with_mock_clock()")
            .advance(duration);
    }

    /// Advance the mock clock by `ms` milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if no clock was attached with [`with_mock_clock`](Self::with_mock_clock).
    pub fn advance_time_ms(&self, ms: u64) {
        self.mock_clock
            .as_ref()
            .expect("advance_time_ms() requires with_mock_clock()")
            .advance_ms(ms);
    }

    /// The execution tracer, if one is attached.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::debugging::ExecutionTracer;
    /// # let tracer = ExecutionTracer::new().enable();
    /// # let env = TestEnv::new().with_tracer(tracer);
    /// if let Some(tracer) = env.tracer() {
    ///     let trace = tracer.get_trace("workflow-123");
    /// }
    /// ```
    pub fn tracer(&self) -> Option<&ExecutionTracer> {
        self.tracer.as_ref()
    }

    /// State snapshots captured for a workflow, oldest first.
    ///
    /// Empty unless snapshots were enabled with [`with_snapshots`](Self::with_snapshots).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # async fn example() {
    /// # let env = TestEnv::new().with_snapshots(true);
    /// let snapshots = env.get_snapshots("workflow-123").await;
    /// println!("Captured {} snapshots", snapshots.len());
    /// # }
    /// ```
    pub async fn get_snapshots(&self, workflow_id: &str) -> Vec<StateSnapshot> {
        let snapshots = self.snapshots.lock().await;
        snapshots.get(workflow_id).cloned().unwrap_or_else(Vec::new)
    }

    /// A [`CommandInspector`] for the commands a workflow context has generated.
    ///
    /// The inspector reads a [`WorkflowContext`], so the test needs the context
    /// after the workflow has run.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::workflow::WorkflowContext;
    /// # fn example(ctx: &WorkflowContext) {
    /// let env = TestEnv::new();
    /// let inspector = env.command_inspector();
    /// let commands = inspector.inspect(ctx);
    /// println!("Generated {} commands", commands.len());
    /// # }
    /// ```
    pub fn command_inspector(&self) -> CommandInspector {
        CommandInspector::new()
    }

    /// Register a workflow that takes no context, under `workflow_type`.
    ///
    /// Input and output pass through JSON. Registering the same type again replaces the
    /// earlier handler.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::prelude::*;
    /// #[derive(Serialize, Deserialize)]
    /// struct OrderInput {
    ///     order_id: String,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.register_workflow("OrderWorkflow", |input: OrderInput| async move {
    ///     Ok(format!("accepted {}", input.order_id))
    /// });
    /// ```
    pub fn register_workflow<F, Fut, I, O>(&mut self, workflow_type: &str, handler: F)
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<O>> + Send + 'static,
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
    {
        let wrapped_handler = Arc::new(move |input_bytes: Vec<u8>| {
            let input: I = match serde_json::from_slice(&input_bytes) {
                Ok(i) => i,
                Err(e) => {
                    return Box::pin(async move {
                        Err(Error::Serialization(format!(
                            "Failed to deserialize input: {}",
                            e
                        )))
                    })
                        as std::pin::Pin<
                            Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>,
                        >;
                }
            };

            let fut = handler(input);

            Box::pin(async move {
                let output = fut.await?;
                let output_bytes = serde_json::to_vec(&output).map_err(|e| {
                    Error::Serialization(format!("Failed to serialize output: {}", e))
                })?;
                Ok(output_bytes)
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>
        });

        let mut workflows = self.workflows.write().unwrap();
        workflows.insert(workflow_type.to_string(), wrapped_handler);
    }

    /// Register a workflow that takes a [`WorkflowContext`], under `workflow_type`.
    ///
    /// When both kinds are registered under the same type, this one runs. A call to
    /// `ctx.execute_task` suspends the workflow, which makes
    /// [`execute_workflow`](Self::execute_workflow) return an error; see [`TestEnv`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::prelude::*;
    /// #[derive(Serialize, Deserialize)]
    /// struct OrderInput {
    ///     order_id: String,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.register_workflow_with_context(
    ///     "OrderWorkflow",
    ///     |ctx: WorkflowContext, input: OrderInput| async move {
    ///         Ok(format!("{} ran as {}", input.order_id, ctx.workflow_type()))
    ///     },
    /// );
    /// ```
    pub fn register_workflow_with_context<F, Fut, I, O>(&mut self, workflow_type: &str, handler: F)
    where
        F: Fn(WorkflowContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
    {
        let wrapped_handler = Arc::new(move |ctx: WorkflowContext, input_bytes: Vec<u8>| {
            let input: I = match serde_json::from_slice(&input_bytes) {
                Ok(i) => i,
                Err(e) => {
                    return Box::pin(async move {
                        Err(Error::Serialization(format!(
                            "Failed to deserialize input: {}",
                            e
                        )))
                    })
                        as Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>;
                }
            };

            let fut = handler(ctx, input);

            Box::pin(async move {
                let output = fut.await?;
                let output_bytes = serde_json::to_vec(&output).map_err(|e| {
                    Error::Serialization(format!("Failed to serialize output: {}", e))
                })?;
                Ok(output_bytes)
            }) as Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>
        });

        let mut workflows = self.context_workflows.write().unwrap();
        workflows.insert(workflow_type.to_string(), wrapped_handler);
    }

    /// Start configuring a mock for the task type `task_type`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use serde::Serialize;
    /// #[derive(Serialize)]
    /// struct PaymentResult {
    ///     success: bool,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.mock_task("process_payment")
    ///     .returns(PaymentResult { success: true });
    /// ```
    pub fn mock_task(&mut self, task_type: &str) -> crate::testing::MockTaskBuilder {
        crate::testing::MockTaskBuilder::new(task_type.to_string(), self.mocks.clone())
    }

    /// Run a registered workflow to completion with `input`.
    ///
    /// Each run gets a fresh `test-wf-<uuid>` workflow ID. The run's status is recorded
    /// as completed or failed, and, when configured, the tracer and snapshots are
    /// updated too.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow type is not registered, if input or output does
    /// not serialize, or if the workflow itself fails or suspends.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::prelude::*;
    /// # async fn example() -> Result<()> {
    /// # let mut env = TestEnv::new();
    /// let result: String = env
    ///     .execute_workflow("MyWorkflow", "input")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_workflow<I, O>(&mut self, workflow_type: &str, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let workflow_id = format!("test-wf-{}", uuid::Uuid::new_v4());

        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| Error::Serialization(format!("Failed to serialize input: {}", e)))?;

        let execution = WorkflowExecution {
            workflow_id: workflow_id.clone(),
            run_id: format!("test-run-{}", uuid::Uuid::new_v4()),
            workflow_type: workflow_type.to_string(),
            attempt: 1,
            namespace: self.namespace.clone(),
            task_queue: self.task_queue.clone(),
        };

        if let Some(tracer) = &self.tracer {
            tracer.start_trace(&workflow_id, workflow_type);
        }

        {
            let mut traces = self.traces.lock().await;
            traces.insert(
                workflow_id.clone(),
                ExecutionTrace {
                    workflow_id: workflow_id.clone(),
                    workflow_type: workflow_type.to_string(),
                    status: ExecutionStatus::Running,
                    state: HashMap::new(),
                    tasks_executed: Vec::new(),
                    commands: Vec::new(),
                    events: Vec::new(),
                    queries: Vec::new(),
                },
            );
        }

        // A context-aware handler takes precedence over a plain one of the same type.
        let context_handler = {
            let workflows = self.context_workflows.read().unwrap();
            workflows.get(workflow_type).cloned()
        };

        let simple_handler = {
            let workflows = self.workflows.read().unwrap();
            workflows.get(workflow_type).cloned()
        };

        let ctx = WorkflowContext::new(execution.clone(), false, "1.0.0".to_string());

        if self.capture_snapshots {
            let snapshot = StateSnapshot::capture_with_label(&ctx, "initial");
            let mut snapshots = self.snapshots.lock().await;
            snapshots
                .entry(workflow_id.clone())
                .or_insert_with(Vec::new)
                .push(snapshot);
        }

        let result_bytes = if let Some(handler) = context_handler {
            // Task mocks are not consulted here: a workflow that calls
            // ctx.execute_task() suspends, and the suspension is returned as an error.
            match handler(ctx, input_bytes).await {
                Ok(output) => {
                    if let Some(tracer) = &self.tracer {
                        tracer.record_event(
                            &workflow_id,
                            crate::debugging::ExecutionEvent::new(
                                crate::debugging::TraceLevel::Info,
                                "workflow_completed",
                                &workflow_id,
                                "Workflow completed successfully",
                            ),
                        );
                        tracer.complete_trace(&workflow_id);
                    }

                    let mut traces = self.traces.lock().await;
                    if let Some(trace) = traces.get_mut(&workflow_id) {
                        trace.status = ExecutionStatus::Completed;
                    }
                    output
                }
                Err(e) => {
                    if let Some(tracer) = &self.tracer {
                        tracer.record_event(
                            &workflow_id,
                            crate::debugging::ExecutionEvent::new(
                                crate::debugging::TraceLevel::Error,
                                "workflow_failed",
                                &workflow_id,
                                format!("Workflow failed: {}", e),
                            ),
                        );
                        tracer.complete_trace(&workflow_id);
                    }

                    let mut traces = self.traces.lock().await;
                    if let Some(trace) = traces.get_mut(&workflow_id) {
                        trace.status = ExecutionStatus::Failed(e.to_string());
                    }
                    return Err(e);
                }
            }
        } else if let Some(handler) = simple_handler {
            match handler(input_bytes).await {
                Ok(output) => {
                    if let Some(tracer) = &self.tracer {
                        tracer.record_event(
                            &workflow_id,
                            crate::debugging::ExecutionEvent::new(
                                crate::debugging::TraceLevel::Info,
                                "workflow_completed",
                                &workflow_id,
                                "Workflow completed successfully",
                            ),
                        );
                        tracer.complete_trace(&workflow_id);
                    }

                    let mut traces = self.traces.lock().await;
                    if let Some(trace) = traces.get_mut(&workflow_id) {
                        trace.status = ExecutionStatus::Completed;
                    }
                    output
                }
                Err(e) => {
                    if let Some(tracer) = &self.tracer {
                        tracer.record_event(
                            &workflow_id,
                            crate::debugging::ExecutionEvent::new(
                                crate::debugging::TraceLevel::Error,
                                "workflow_failed",
                                &workflow_id,
                                format!("Workflow failed: {}", e),
                            ),
                        );
                        tracer.complete_trace(&workflow_id);
                    }

                    let mut traces = self.traces.lock().await;
                    if let Some(trace) = traces.get_mut(&workflow_id) {
                        trace.status = ExecutionStatus::Failed(e.to_string());
                    }
                    return Err(e);
                }
            }
        } else {
            return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                format!(
                    "Workflow '{}' not registered. Use register_workflow() or register_workflow_with_context().",
                    workflow_type
                )
            )));
        };

        let result: O = serde_json::from_slice(&result_bytes)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize output: {}", e)))?;

        Ok(result)
    }

    /// Read a workflow state value, deserialized from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow or key is unknown, or the value does not
    /// deserialize as `T`.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv, workflow_id: &str) -> orcher::Result<()> {
    /// let counter: i32 = env.get_workflow_state(workflow_id, "counter")?;
    /// assert_eq!(counter, 42);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_workflow_state<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<T> {
        let traces = self.traces.blocking_lock();
        let trace = traces.get(workflow_id).ok_or_else(|| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "Workflow '{}' not found",
                workflow_id
            )))
        })?;

        let value_bytes = trace.state.get(key).ok_or_else(|| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "State key '{}' not found",
                key
            )))
        })?;

        let value: T = serde_json::from_slice(value_bytes)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize state: {}", e)))?;

        Ok(value)
    }

    /// All state keys recorded for a workflow, or an empty list if it is unknown.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv, workflow_id: &str) {
    /// let keys = env.workflow_state_keys(workflow_id);
    /// assert!(keys.contains(&"counter".to_string()));
    /// # }
    /// ```
    pub fn workflow_state_keys(&self, workflow_id: &str) -> Vec<String> {
        let traces = self.traces.blocking_lock();
        traces
            .get(workflow_id)
            .map(|trace| trace.state.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Types of the tasks a workflow executed, in order.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv, workflow_id: &str) {
    /// let tasks = env.executed_tasks(workflow_id);
    /// assert_eq!(tasks.len(), 3);
    /// # }
    /// ```
    pub fn executed_tasks(&self, workflow_id: &str) -> Vec<String> {
        let traces = self.traces.blocking_lock();
        traces
            .get(workflow_id)
            .map(|trace| trace.tasks_executed.clone())
            .unwrap_or_default()
    }

    /// The status of a workflow run, or `None` if the ID is unknown.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use orcher::testing::ExecutionStatus;
    /// # fn example(env: &TestEnv, workflow_id: &str) {
    /// let status = env.workflow_status(workflow_id);
    /// assert_eq!(status, Some(ExecutionStatus::Completed));
    /// # }
    /// ```
    pub fn workflow_status(&self, workflow_id: &str) -> Option<ExecutionStatus> {
        let traces = self.traces.blocking_lock();
        traces.get(workflow_id).map(|trace| trace.status.clone())
    }

    /// How many times a task type was called, across all runs.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv) {
    /// let count = env.task_call_count("process_payment");
    /// assert_eq!(count, 1);
    /// # }
    /// ```
    pub fn task_call_count(&self, task_type: &str) -> usize {
        let calls = self.task_calls.blocking_lock();
        calls.get(task_type).map(|v| v.len()).unwrap_or(0)
    }

    /// Every recorded call of a task type, in call order.
    ///
    /// # Panics
    ///
    /// Panics if called from inside an async runtime (it takes a blocking lock).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv) {
    /// let calls = env.task_calls("process_payment");
    /// for call in calls {
    ///     println!("Task called at {:?}", call.timestamp);
    /// }
    /// # }
    /// ```
    pub fn task_calls(&self, task_type: &str) -> Vec<TaskCall> {
        let calls = self.task_calls.blocking_lock();
        calls.get(task_type).cloned().unwrap_or_default()
    }

    /// Assert that a task type was called exactly `expected_count` times.
    ///
    /// # Panics
    ///
    /// Panics if the count differs, or if called from inside an async runtime.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv) {
    /// env.assert_task_called("process_payment", 1);
    /// # }
    /// ```
    pub fn assert_task_called(&self, task_type: &str, expected_count: usize) {
        let actual_count = self.task_call_count(task_type);
        assert_eq!(
            actual_count, expected_count,
            "Expected task '{}' to be called {} times, but was called {} times",
            task_type, expected_count, actual_count
        );
    }

    /// Assert that a workflow completed successfully.
    ///
    /// # Panics
    ///
    /// Panics if the workflow did not complete or is not found, or if called from
    /// inside an async runtime.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv, workflow_id: &str) {
    /// env.assert_workflow_completed(workflow_id);
    /// # }
    /// ```
    pub fn assert_workflow_completed(&self, workflow_id: &str) {
        let status = self.workflow_status(workflow_id);
        assert_eq!(
            status,
            Some(ExecutionStatus::Completed),
            "Expected workflow '{}' to be completed, but status was {:?}",
            workflow_id,
            status
        );
    }

    /// Assert that a workflow state key holds the expected value.
    ///
    /// # Panics
    ///
    /// Panics if the key is missing or the value differs, or if called from inside an
    /// async runtime.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # fn example(env: &TestEnv, workflow_id: &str) {
    /// env.assert_state_equals(workflow_id, "status", &"completed".to_string());
    /// # }
    /// ```
    pub fn assert_state_equals<T>(&self, workflow_id: &str, key: &str, expected: &T)
    where
        T: DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let actual: T = self
            .get_workflow_state(workflow_id, key)
            .unwrap_or_else(|_| panic!("Failed to get state key '{}'", key));

        assert_eq!(
            actual, *expected,
            "Expected state['{}'] to be {:?}, but was {:?}",
            key, expected, actual
        );
    }

    /// Record a call of `task_type` with the given JSON input.
    #[allow(dead_code)] // Only the unit tests call it
    pub(crate) fn record_task_call(&self, task_type: String, input: Vec<u8>) {
        let mut calls = self.task_calls.blocking_lock();
        calls.entry(task_type.clone()).or_default().push(TaskCall {
            task_type,
            input,
            timestamp: std::time::Instant::now(),
        });
    }
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_env_creation() {
        let env = TestEnv::new();
        assert_eq!(env.namespace, "test");
        assert_eq!(env.task_queue, "test-queue");
    }

    #[test]
    fn test_test_env_with_namespace() {
        let env = TestEnv::new().with_namespace("custom");
        assert_eq!(env.namespace, "custom");
    }

    #[test]
    fn test_test_env_with_task_queue() {
        let env = TestEnv::new().with_task_queue("custom-queue");
        assert_eq!(env.task_queue, "custom-queue");
    }

    #[test]
    fn test_task_call_count() {
        let env = TestEnv::new();
        env.record_task_call("task1".to_string(), vec![]);
        env.record_task_call("task1".to_string(), vec![]);
        env.record_task_call("task2".to_string(), vec![]);

        assert_eq!(env.task_call_count("task1"), 2);
        assert_eq!(env.task_call_count("task2"), 1);
        assert_eq!(env.task_call_count("task3"), 0);
    }

    #[test]
    fn test_execution_status() {
        assert_eq!(ExecutionStatus::Running, ExecutionStatus::Running);
        assert_eq!(ExecutionStatus::Completed, ExecutionStatus::Completed);
        assert_ne!(ExecutionStatus::Running, ExecutionStatus::Completed);
    }

    #[tokio::test]
    async fn test_register_and_execute_workflow() {
        let mut env = TestEnv::new();

        env.register_workflow("TestWorkflow", |input: String| async move {
            Ok(format!("Processed: {}", input))
        });

        let result: String = env
            .execute_workflow("TestWorkflow", "test input".to_string())
            .await
            .unwrap();

        assert_eq!(result, "Processed: test input");
    }

    #[tokio::test]
    async fn test_workflow_not_registered() {
        let mut env = TestEnv::new();

        let result: Result<String> = env
            .execute_workflow("UnknownWorkflow", "input".to_string())
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_workflow_with_complex_types() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Input {
            value: i32,
        }

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Output {
            result: i32,
        }

        let mut env = TestEnv::new();

        env.register_workflow("ComplexWorkflow", |input: Input| async move {
            Ok(Output {
                result: input.value * 2,
            })
        });

        let result: Output = env
            .execute_workflow("ComplexWorkflow", Input { value: 21 })
            .await
            .unwrap();

        assert_eq!(result, Output { result: 42 });
    }

    #[tokio::test]
    async fn test_context_aware_workflow() {
        let mut env = TestEnv::new();

        env.register_workflow_with_context(
            "ContextWorkflow",
            |_ctx: WorkflowContext, input: i32| async move { Ok(input * 2) },
        );

        let result: i32 = env.execute_workflow("ContextWorkflow", 21).await.unwrap();

        assert_eq!(result, 42);
    }

    #[test]
    fn test_mock_clock_integration() {
        use crate::testing::MockClock;
        use std::time::Duration;

        let clock = MockClock::new();
        let env = TestEnv::new().with_mock_clock(clock.clone());

        assert!(env.clock().is_some());
        assert_eq!(env.clock().unwrap().elapsed_ms(), 0);

        clock.advance(Duration::from_secs(5));
        assert_eq!(env.clock().unwrap().elapsed_ms(), 5000);

        env.advance_time(Duration::from_secs(3));
        assert_eq!(clock.elapsed_ms(), 8000);

        env.advance_time_ms(2000);
        assert_eq!(clock.elapsed_ms(), 10000);
    }

    #[test]
    fn test_no_mock_clock() {
        let env = TestEnv::new();
        assert!(env.clock().is_none());
    }

    #[tokio::test]
    async fn test_workflow_with_both_registrations() {
        let mut env = TestEnv::new();

        env.register_workflow("SimpleWorkflow", |input: String| async move {
            Ok(format!("Simple: {}", input))
        });

        env.register_workflow_with_context(
            "ContextWorkflow",
            |_ctx: WorkflowContext, input: String| async move { Ok(format!("Context: {}", input)) },
        );

        let result1: String = env
            .execute_workflow("SimpleWorkflow", "test".to_string())
            .await
            .unwrap();

        let result2: String = env
            .execute_workflow("ContextWorkflow", "test".to_string())
            .await
            .unwrap();

        assert_eq!(result1, "Simple: test");
        assert_eq!(result2, "Context: test");
    }
}
