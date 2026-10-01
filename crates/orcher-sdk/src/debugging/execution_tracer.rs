//! In-memory recording of workflow execution events, per workflow ID.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Severity of an execution event.
///
/// Levels are ordered from most to least severe: `Error < Warn < Info < Debug < Trace`.
/// A filter at a given level keeps that level and everything more severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TraceLevel {
    /// Failures.
    Error,
    /// Unexpected but recoverable conditions.
    Warn,
    /// Normal progress.
    Info,
    /// Detail useful when debugging, such as generated commands.
    Debug,
    /// The most verbose detail.
    Trace,
}

impl From<tracing::Level> for TraceLevel {
    fn from(level: tracing::Level) -> Self {
        match level {
            tracing::Level::ERROR => TraceLevel::Error,
            tracing::Level::WARN => TraceLevel::Warn,
            tracing::Level::INFO => TraceLevel::Info,
            tracing::Level::DEBUG => TraceLevel::Debug,
            tracing::Level::TRACE => TraceLevel::Trace,
        }
    }
}

/// A single event recorded during workflow execution.
#[derive(Debug, Clone)]
pub struct ExecutionEvent {
    /// Wall-clock time at which the event was created.
    pub timestamp: SystemTime,

    /// Severity of the event.
    pub level: TraceLevel,

    /// Machine-readable event kind, such as `"task_scheduled"` or `"timer_started"`.
    pub kind: String,

    /// ID of the workflow the event belongs to.
    pub workflow_id: String,

    /// Human-readable description.
    pub message: String,

    /// Free-form key/value details.
    pub metadata: HashMap<String, String>,
}

impl ExecutionEvent {
    /// Create an event timestamped with the current wall-clock time.
    pub fn new(
        level: TraceLevel,
        kind: impl Into<String>,
        workflow_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            level,
            kind: kind.into(),
            workflow_id: workflow_id.into(),
            message: message.into(),
            metadata: HashMap::new(),
        }
    }

    /// Attach a metadata entry, replacing any existing value for `key` (builder-style).
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Return the time since the event was created, or zero if the clock went backwards.
    pub fn elapsed(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.timestamp)
            .unwrap_or(Duration::ZERO)
    }
}

/// Everything recorded for one workflow: events, a command count and the state
/// keys it accessed.
#[derive(Debug, Clone)]
pub struct ExecutionTrace {
    /// ID of the traced workflow.
    pub workflow_id: String,

    /// Type name of the traced workflow.
    pub workflow_type: String,

    /// Wall-clock time at which the trace started.
    pub started_at: SystemTime,

    /// Wall-clock time at which the trace was completed, if it has been.
    pub ended_at: Option<SystemTime>,

    /// Recorded events, in the order they were recorded.
    pub events: Vec<ExecutionEvent>,

    /// Number of commands recorded.
    pub command_count: usize,

    /// Distinct state keys accessed, in order of first access.
    pub state_keys: Vec<String>,
}

impl ExecutionTrace {
    /// Create an empty trace that starts now.
    pub fn new(workflow_id: impl Into<String>, workflow_type: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            workflow_type: workflow_type.into(),
            started_at: SystemTime::now(),
            ended_at: None,
            events: Vec::new(),
            command_count: 0,
            state_keys: Vec::new(),
        }
    }

    /// Append an event.
    pub fn add_event(&mut self, event: ExecutionEvent) {
        self.events.push(event);
    }

    /// Mark the trace as completed now.
    pub fn complete(&mut self) {
        self.ended_at = Some(SystemTime::now());
    }

    /// Return the time from start to completion, or `None` if the trace is not completed.
    pub fn duration(&self) -> Option<Duration> {
        self.ended_at
            .and_then(|end| end.duration_since(self.started_at).ok())
    }

    /// Return the events at `level` or more severe.
    ///
    /// Filtering at [`TraceLevel::Info`] returns error, warning and info events.
    pub fn events_at_level(&self, level: TraceLevel) -> Vec<&ExecutionEvent> {
        self.events.iter().filter(|e| e.level <= level).collect()
    }

    /// Return the events of the given kind.
    pub fn events_of_kind(&self, kind: &str) -> Vec<&ExecutionEvent> {
        self.events.iter().filter(|e| e.kind == kind).collect()
    }

    /// Return the number of recorded events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Return whether any error event was recorded.
    pub fn has_errors(&self) -> bool {
        self.events.iter().any(|e| e.level == TraceLevel::Error)
    }

    /// Return the error events.
    pub fn errors(&self) -> Vec<&ExecutionEvent> {
        self.events
            .iter()
            .filter(|e| e.level == TraceLevel::Error)
            .collect()
    }
}

/// Records execution traces for workflows, keyed by workflow ID.
///
/// A tracer is disabled when created and records nothing until
/// [`enable`](Self::enable) is called. Clones share the same set of traces.
/// Recording calls for a workflow with no started trace are ignored.
#[derive(Clone)]
pub struct ExecutionTracer {
    /// Least severe event level that is recorded.
    level: TraceLevel,

    capture_spans: bool,

    capture_events: bool,

    /// Traces by workflow ID, shared between clones.
    traces: Arc<Mutex<HashMap<String, ExecutionTrace>>>,

    enabled: bool,
}

impl ExecutionTracer {
    /// Create a disabled tracer that records events at [`TraceLevel::Info`] and above.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::debugging::ExecutionTracer;
    ///
    /// let tracer = ExecutionTracer::new();
    /// ```
    pub fn new() -> Self {
        Self {
            level: TraceLevel::Info,
            capture_spans: true,
            capture_events: true,
            traces: Arc::new(Mutex::new(HashMap::new())),
            enabled: false,
        }
    }

    /// Set the least severe level that is recorded (builder-style).
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::debugging::{ExecutionTracer, TraceLevel};
    ///
    /// let tracer = ExecutionTracer::new()
    ///     .with_level(TraceLevel::Debug);
    /// ```
    pub fn with_level(mut self, level: TraceLevel) -> Self {
        self.level = level;
        self
    }

    /// Set whether spans are captured (builder-style).
    ///
    /// The flag is stored, but no span data is recorded.
    pub fn with_spans(mut self, capture: bool) -> Self {
        self.capture_spans = capture;
        self
    }

    /// Set whether events passed to [`record_event`](Self::record_event) are kept
    /// (builder-style).
    pub fn with_events(mut self, capture: bool) -> Self {
        self.capture_events = capture;
        self
    }

    /// Enable recording (builder-style).
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::debugging::ExecutionTracer;
    ///
    /// let tracer = ExecutionTracer::new().enable();
    /// ```
    pub fn enable(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Disable recording (builder-style). Traces already recorded are kept.
    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Return whether recording is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Start a trace for a workflow, replacing any existing trace for the same ID.
    pub fn start_trace(&self, workflow_id: impl Into<String>, workflow_type: impl Into<String>) {
        if !self.enabled {
            return;
        }

        let workflow_id = workflow_id.into();
        let workflow_type = workflow_type.into();

        let mut traces = self.traces.lock().unwrap();
        traces.insert(
            workflow_id.clone(),
            ExecutionTrace::new(workflow_id, workflow_type),
        );
    }

    /// Record an event, unless it is less severe than the tracer's level.
    pub fn record_event(&self, workflow_id: &str, event: ExecutionEvent) {
        if !self.enabled || !self.capture_events {
            return;
        }

        if event.level > self.level {
            return;
        }

        let mut traces = self.traces.lock().unwrap();
        if let Some(trace) = traces.get_mut(workflow_id) {
            trace.add_event(event);
        }
    }

    /// Count a generated command and record a debug-level `command_generated` event.
    ///
    /// The event is added regardless of the tracer's level.
    pub fn record_command(&self, workflow_id: &str, command_type: &str) {
        if !self.enabled {
            return;
        }

        let mut traces = self.traces.lock().unwrap();
        if let Some(trace) = traces.get_mut(workflow_id) {
            trace.command_count += 1;

            let event = ExecutionEvent::new(
                TraceLevel::Debug,
                "command_generated",
                workflow_id,
                format!("Command generated: {}", command_type),
            )
            .with_metadata("command_type", command_type);

            trace.add_event(event);
        }
    }

    /// Record that a state key was accessed. Repeated keys are recorded once.
    pub fn record_state_access(&self, workflow_id: &str, key: &str) {
        if !self.enabled {
            return;
        }

        let mut traces = self.traces.lock().unwrap();
        if let Some(trace) = traces.get_mut(workflow_id) {
            if !trace.state_keys.contains(&key.to_string()) {
                trace.state_keys.push(key.to_string());
            }
        }
    }

    /// Mark a workflow's trace as completed now.
    pub fn complete_trace(&self, workflow_id: &str) {
        if !self.enabled {
            return;
        }

        let mut traces = self.traces.lock().unwrap();
        if let Some(trace) = traces.get_mut(workflow_id) {
            trace.complete();
        }
    }

    /// Return a copy of the trace for a workflow, if one was started.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::debugging::ExecutionTracer;
    /// # let tracer = ExecutionTracer::new();
    /// let trace = tracer.get_trace("workflow-123");
    /// if let Some(trace) = trace {
    ///     println!("Events: {}", trace.event_count());
    ///     println!("Commands: {}", trace.command_count);
    /// }
    /// ```
    pub fn get_trace(&self, workflow_id: &str) -> Option<ExecutionTrace> {
        let traces = self.traces.lock().unwrap();
        traces.get(workflow_id).cloned()
    }

    /// Return copies of all traces, in no particular order.
    pub fn all_traces(&self) -> Vec<ExecutionTrace> {
        let traces = self.traces.lock().unwrap();
        traces.values().cloned().collect()
    }

    /// Remove all traces.
    pub fn clear(&self) {
        let mut traces = self.traces.lock().unwrap();
        traces.clear();
    }

    /// Return the number of traces held.
    pub fn trace_count(&self) -> usize {
        let traces = self.traces.lock().unwrap();
        traces.len()
    }
}

impl Default for ExecutionTracer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_tracer_creation() {
        let tracer = ExecutionTracer::new();
        assert!(!tracer.is_enabled());
        assert_eq!(tracer.level, TraceLevel::Info);
    }

    #[test]
    fn test_execution_tracer_enable() {
        let tracer = ExecutionTracer::new().enable();
        assert!(tracer.is_enabled());
    }

    #[test]
    fn test_execution_tracer_with_level() {
        let tracer = ExecutionTracer::new().with_level(TraceLevel::Debug);
        assert_eq!(tracer.level, TraceLevel::Debug);
    }

    #[test]
    fn test_start_and_get_trace() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-123", "TestWorkflow");

        let trace = tracer.get_trace("wf-123");
        assert!(trace.is_some());

        let trace = trace.unwrap();
        assert_eq!(trace.workflow_id, "wf-123");
        assert_eq!(trace.workflow_type, "TestWorkflow");
        assert_eq!(trace.event_count(), 0);
    }

    #[test]
    fn test_record_event() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-123", "TestWorkflow");

        let event = ExecutionEvent::new(TraceLevel::Info, "test_event", "wf-123", "Test message");
        tracer.record_event("wf-123", event);

        let trace = tracer.get_trace("wf-123").unwrap();
        assert_eq!(trace.event_count(), 1);
        assert_eq!(trace.events[0].kind, "test_event");
    }

    #[test]
    fn test_record_command() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-123", "TestWorkflow");

        tracer.record_command("wf-123", "ScheduleTask");
        tracer.record_command("wf-123", "StartTimer");

        let trace = tracer.get_trace("wf-123").unwrap();
        assert_eq!(trace.command_count, 2);
    }

    #[test]
    fn test_record_state_access() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-123", "TestWorkflow");

        tracer.record_state_access("wf-123", "counter");
        tracer.record_state_access("wf-123", "status");
        tracer.record_state_access("wf-123", "counter"); // Already recorded

        let trace = tracer.get_trace("wf-123").unwrap();
        assert_eq!(trace.state_keys.len(), 2);
        assert!(trace.state_keys.contains(&"counter".to_string()));
        assert!(trace.state_keys.contains(&"status".to_string()));
    }

    #[test]
    fn test_complete_trace() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-123", "TestWorkflow");

        let trace_before = tracer.get_trace("wf-123").unwrap();
        assert!(trace_before.ended_at.is_none());

        tracer.complete_trace("wf-123");

        let trace_after = tracer.get_trace("wf-123").unwrap();
        assert!(trace_after.ended_at.is_some());
        assert!(trace_after.duration().is_some());
    }

    #[test]
    fn test_event_filtering() {
        let tracer = ExecutionTracer::new()
            .enable()
            .with_level(TraceLevel::Debug);
        tracer.start_trace("wf-123", "TestWorkflow");

        tracer.record_event(
            "wf-123",
            ExecutionEvent::new(TraceLevel::Error, "error", "wf-123", "Error message"),
        );
        tracer.record_event(
            "wf-123",
            ExecutionEvent::new(TraceLevel::Info, "info", "wf-123", "Info message"),
        );
        tracer.record_event(
            "wf-123",
            ExecutionEvent::new(TraceLevel::Debug, "debug", "wf-123", "Debug message"),
        );

        let trace = tracer.get_trace("wf-123").unwrap();
        assert_eq!(trace.event_count(), 3);

        let errors = trace.errors();
        assert_eq!(errors.len(), 1);

        let info_events = trace.events_at_level(TraceLevel::Info);
        assert_eq!(info_events.len(), 2); // Error and Info; Debug is excluded
    }

    #[test]
    fn test_trace_level_ordering() {
        assert!(TraceLevel::Error < TraceLevel::Warn);
        assert!(TraceLevel::Warn < TraceLevel::Info);
        assert!(TraceLevel::Info < TraceLevel::Debug);
        assert!(TraceLevel::Debug < TraceLevel::Trace);
    }

    #[test]
    fn test_disabled_tracer() {
        let tracer = ExecutionTracer::new(); // Not enabled
        tracer.start_trace("wf-123", "TestWorkflow");

        let trace = tracer.get_trace("wf-123");
        assert!(trace.is_none());
    }

    #[test]
    fn test_clear_traces() {
        let tracer = ExecutionTracer::new().enable();
        tracer.start_trace("wf-1", "Test1");
        tracer.start_trace("wf-2", "Test2");

        assert_eq!(tracer.trace_count(), 2);

        tracer.clear();

        assert_eq!(tracer.trace_count(), 0);
    }
}
