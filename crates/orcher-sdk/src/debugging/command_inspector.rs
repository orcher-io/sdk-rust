//! Readable views of the commands a workflow has generated.

use crate::workflow::{WorkflowCommand, WorkflowContext};
use std::time::SystemTime;

/// A readable summary of one workflow command.
#[derive(Debug, Clone)]
pub struct CommandInfo {
    /// Zero-based position of the command in the workflow's command list.
    pub sequence: u64,

    /// Name of the command variant, such as `"ScheduleTask"`.
    pub command_type: String,

    /// Wall-clock time at which this summary was built, not when the command was emitted.
    pub timestamp: SystemTime,

    /// Fields specific to the command type.
    pub details: CommandDetails,
}

/// Fields specific to each command type.
#[derive(Debug, Clone)]
pub enum CommandDetails {
    /// A task was scheduled.
    ScheduleTask {
        /// The task's id within the workflow.
        task_id: String,
        /// The task type, the name it is registered under.
        task_type: String,
        /// The task queue it was scheduled on.
        task_queue: String,
    },

    /// A timer was started. The duration is truncated to whole seconds.
    StartTimer {
        /// The timer's id within the workflow.
        timer_id: String,
        /// How long the timer runs, in whole seconds.
        duration_secs: u64,
    },

    /// A child workflow was started.
    StartChildWorkflow {
        /// The child's workflow id.
        workflow_id: String,
        /// The child's workflow type.
        workflow_type: String,
    },

    /// An event was sent to another workflow.
    SendEvent {
        /// The receiving workflow's id.
        workflow_id: String,
        /// The event's name.
        event_name: String,
    },

    /// Cancellation of a child workflow was requested.
    CancelChildWorkflow {
        /// The child's workflow id.
        workflow_id: String,
    },

    /// The workflow began waiting for an event.
    WaitForEvent {
        /// The event's name.
        event_name: String,
        /// Whether the wait gives up after a timeout.
        has_timeout: bool,
    },

    /// The workflow restarts with fresh history, optionally as a different type.
    RestartFresh {
        /// The workflow type of the new run, if it changes.
        new_workflow_type: Option<String>,
    },

    /// Any other command, described in text.
    Other {
        /// A readable description of the command.
        description: String,
    },
}

impl CommandInfo {
    /// Summarize a workflow command found at position `sequence`.
    pub fn from_command(command: &WorkflowCommand, sequence: u64) -> Self {
        let (command_type, details) = match command {
            WorkflowCommand::RecordStepResult(cmd) => (
                "RecordStepResult".to_string(),
                CommandDetails::Other {
                    description: format!("Record step result: {}", cmd.step_name),
                },
            ),
            WorkflowCommand::ScheduleTask(cmd) => (
                "ScheduleTask".to_string(),
                CommandDetails::ScheduleTask {
                    task_id: cmd.task_id.clone(),
                    task_type: cmd.task_type.clone(),
                    task_queue: cmd.task_queue.clone(),
                },
            ),
            WorkflowCommand::StartTimer(cmd) => (
                "StartTimer".to_string(),
                CommandDetails::StartTimer {
                    timer_id: cmd.timer_id.clone(),
                    duration_secs: cmd.duration.as_secs(),
                },
            ),
            WorkflowCommand::StartChildWorkflow(cmd) => (
                "StartChildWorkflow".to_string(),
                CommandDetails::StartChildWorkflow {
                    workflow_id: cmd.workflow_id.clone(),
                    workflow_type: cmd.workflow_type.clone(),
                },
            ),
            WorkflowCommand::SendEvent(cmd) => (
                "SendEvent".to_string(),
                CommandDetails::SendEvent {
                    workflow_id: cmd.workflow_id.clone(),
                    event_name: cmd.event_name.clone(),
                },
            ),
            WorkflowCommand::CancelChildWorkflow(cmd) => (
                "CancelChildWorkflow".to_string(),
                CommandDetails::CancelChildWorkflow {
                    workflow_id: cmd.workflow_id.clone(),
                },
            ),
            WorkflowCommand::WaitForEvent(cmd) => (
                "WaitForEvent".to_string(),
                CommandDetails::WaitForEvent {
                    event_name: cmd.event_name.clone(),
                    has_timeout: cmd.timeout.is_some(),
                },
            ),
            WorkflowCommand::RestartFresh(cmd) => (
                "RestartFresh".to_string(),
                CommandDetails::RestartFresh {
                    new_workflow_type: cmd.workflow_type.clone(),
                },
            ),
            WorkflowCommand::CompleteWorkflow(_) => (
                "CompleteWorkflow".to_string(),
                CommandDetails::Other {
                    description: "Workflow completed successfully".to_string(),
                },
            ),
            WorkflowCommand::FailWorkflow(cmd) => (
                "FailWorkflow".to_string(),
                CommandDetails::Other {
                    description: format!("Workflow failed: {}", cmd.message),
                },
            ),
            WorkflowCommand::CancelTimer(cmd) => (
                "CancelTimer".to_string(),
                CommandDetails::Other {
                    description: format!("Cancel timer: {}", cmd.timer_id),
                },
            ),
        };

        Self {
            sequence,
            command_type,
            timestamp: SystemTime::now(),
            details,
        }
    }

    /// Return a one-line description of the command.
    pub fn description(&self) -> String {
        match &self.details {
            CommandDetails::ScheduleTask {
                task_type, task_id, ..
            } => {
                format!("Schedule task '{}' (id: {})", task_type, task_id)
            }
            CommandDetails::StartTimer {
                timer_id,
                duration_secs,
            } => {
                format!("Start timer '{}' for {}s", timer_id, duration_secs)
            }
            CommandDetails::StartChildWorkflow {
                workflow_type,
                workflow_id,
            } => {
                format!(
                    "Start child workflow '{}' (id: {})",
                    workflow_type, workflow_id
                )
            }
            CommandDetails::SendEvent {
                workflow_id,
                event_name,
            } => {
                format!("Send event '{}' to {}", event_name, workflow_id)
            }
            CommandDetails::CancelChildWorkflow { workflow_id } => {
                format!("Cancel child workflow {}", workflow_id)
            }
            CommandDetails::WaitForEvent {
                event_name,
                has_timeout,
            } => {
                if *has_timeout {
                    format!("Wait for event '{}' with timeout", event_name)
                } else {
                    format!("Wait for event '{}'", event_name)
                }
            }
            CommandDetails::RestartFresh {
                new_workflow_type: Some(wf_type),
            } => {
                format!("Restart fresh as workflow type '{}'", wf_type)
            }
            CommandDetails::RestartFresh {
                new_workflow_type: None,
            } => "Restart fresh (same type)".to_string(),
            CommandDetails::Other { description } => description.clone(),
        }
    }

    /// Return whether the command schedules a task.
    pub fn is_task(&self) -> bool {
        matches!(self.details, CommandDetails::ScheduleTask { .. })
    }

    /// Return whether the command starts a timer.
    pub fn is_timer(&self) -> bool {
        matches!(self.details, CommandDetails::StartTimer { .. })
    }

    /// Return whether the command starts a child workflow.
    pub fn is_child_workflow(&self) -> bool {
        matches!(self.details, CommandDetails::StartChildWorkflow { .. })
    }

    /// Return whether the command completes or fails the workflow.
    pub fn is_terminal(&self) -> bool {
        self.command_type == "CompleteWorkflow" || self.command_type == "FailWorkflow"
    }
}

/// Reads the commands a workflow context has emitted and summarizes them.
///
/// Inspection copies the commands; it does not consume or alter them.
pub struct CommandInspector {
    /// Whether complete and fail commands are included in results.
    include_terminal: bool,
}

impl CommandInspector {
    /// Create an inspector that includes terminal commands.
    ///
    /// # Example
    ///
    /// ```rust
    /// use orcher_sdk::debugging::CommandInspector;
    ///
    /// let inspector = CommandInspector::new();
    /// ```
    pub fn new() -> Self {
        Self {
            include_terminal: true,
        }
    }

    /// Set whether complete and fail commands are included (builder-style).
    pub fn with_terminal_commands(mut self, include: bool) -> Self {
        self.include_terminal = include;
        self
    }

    /// Summarize the commands a workflow context has emitted so far, in order.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher_sdk::debugging::CommandInspector;
    /// # use orcher_sdk::workflow::WorkflowContext;
    /// # fn example(ctx: &WorkflowContext) {
    /// let inspector = CommandInspector::new();
    /// let commands = inspector.inspect(ctx);
    ///
    /// for cmd in commands {
    ///     println!("{}: {}", cmd.sequence, cmd.description());
    /// }
    /// # }
    /// ```
    pub fn inspect(&self, ctx: &WorkflowContext) -> Vec<CommandInfo> {
        let commands = ctx.commands_for_test();
        self.inspect_commands(&commands)
    }

    /// Summarize a list of workflow commands.
    ///
    /// Sequence numbers are positions in `commands`, so they stay stable when
    /// terminal commands are filtered out.
    pub fn inspect_commands(&self, commands: &[WorkflowCommand]) -> Vec<CommandInfo> {
        commands
            .iter()
            .enumerate()
            .map(|(idx, cmd)| CommandInfo::from_command(cmd, idx as u64))
            .filter(|info| self.include_terminal || !info.is_terminal())
            .collect()
    }

    /// Return only the task commands.
    pub fn tasks(&self, ctx: &WorkflowContext) -> Vec<CommandInfo> {
        self.inspect(ctx)
            .into_iter()
            .filter(|info| info.is_task())
            .collect()
    }

    /// Return only the timer commands.
    pub fn timers(&self, ctx: &WorkflowContext) -> Vec<CommandInfo> {
        self.inspect(ctx)
            .into_iter()
            .filter(|info| info.is_timer())
            .collect()
    }

    /// Return only the child workflow commands.
    pub fn child_workflows(&self, ctx: &WorkflowContext) -> Vec<CommandInfo> {
        self.inspect(ctx)
            .into_iter()
            .filter(|info| info.is_child_workflow())
            .collect()
    }

    /// Count the context's commands by type.
    ///
    /// Commands that map to [`CommandDetails::Other`] other than complete and fail,
    /// such as recorded step results and timer cancellations, are not counted.
    pub fn summary(&self, ctx: &WorkflowContext) -> CommandSummary {
        let commands = self.inspect(ctx);

        let mut summary = CommandSummary::default();

        for cmd in commands {
            match &cmd.details {
                CommandDetails::ScheduleTask { .. } => summary.task_count += 1,
                CommandDetails::StartTimer { .. } => summary.timer_count += 1,
                CommandDetails::StartChildWorkflow { .. } => summary.child_workflow_count += 1,
                CommandDetails::SendEvent { .. } => summary.event_count += 1,
                CommandDetails::WaitForEvent { .. } => summary.wait_event_count += 1,
                CommandDetails::CancelChildWorkflow { .. } => {}
                CommandDetails::RestartFresh { .. } => summary.start_refresh = true,
                CommandDetails::Other { .. } => {
                    if cmd.command_type == "CompleteWorkflow" {
                        summary.completed = true;
                    } else if cmd.command_type == "FailWorkflow" {
                        summary.failed = true;
                    }
                }
            }
        }

        summary.total_commands = summary.task_count
            + summary.timer_count
            + summary.child_workflow_count
            + summary.event_count
            + summary.wait_event_count
            + if summary.start_refresh { 1 } else { 0 }
            + if summary.completed { 1 } else { 0 }
            + if summary.failed { 1 } else { 0 };

        summary
    }
}

impl Default for CommandInspector {
    fn default() -> Self {
        Self::new()
    }
}

/// Counts of a workflow's commands by type, produced by [`CommandInspector::summary`].
#[derive(Debug, Clone, Default)]
pub struct CommandSummary {
    /// Sum of the counted commands.
    pub total_commands: usize,

    /// Number of tasks scheduled.
    pub task_count: usize,

    /// Number of timers started.
    pub timer_count: usize,

    /// Number of child workflows started.
    pub child_workflow_count: usize,

    /// Number of events sent.
    pub event_count: usize,

    /// Number of waits for an event.
    pub wait_event_count: usize,

    /// Whether the workflow issued a restart-fresh command.
    pub start_refresh: bool,

    /// Whether the workflow issued a complete command.
    pub completed: bool,

    /// Whether the workflow issued a fail command.
    pub failed: bool,
}

impl CommandSummary {
    /// Return whether the workflow has neither completed nor failed.
    pub fn is_running(&self) -> bool {
        !self.completed && !self.failed
    }

    /// Return a one-line summary of the counts.
    pub fn summary(&self) -> String {
        format!(
            "Commands: {} total ({} tasks, {} timers, {} child workflows, {} events)",
            self.total_commands,
            self.task_count,
            self.timer_count,
            self.child_workflow_count,
            self.event_count
        )
    }
}

impl std::fmt::Display for CommandSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{WorkflowContext, WorkflowExecution};

    fn create_test_context() -> WorkflowContext {
        let execution = WorkflowExecution {
            workflow_id: "test-wf".to_string(),
            run_id: "test-run".to_string(),
            workflow_type: "TestWorkflow".to_string(),
            attempt: 1,
            namespace: "test".to_string(),
            task_queue: "test-queue".to_string(),
        };
        WorkflowContext::new(execution, false, "1.0.0".to_string())
    }

    #[test]
    fn test_command_inspector_creation() {
        let inspector = CommandInspector::new();
        assert!(inspector.include_terminal);
    }

    #[test]
    fn test_command_inspector_with_terminal_commands() {
        let inspector = CommandInspector::new().with_terminal_commands(false);
        assert!(!inspector.include_terminal);
    }

    #[test]
    fn test_inspect_empty_context() {
        let ctx = create_test_context();
        let inspector = CommandInspector::new();
        let commands = inspector.inspect(&ctx);
        assert_eq!(commands.len(), 0);
    }

    #[test]
    fn test_command_info_description() {
        let info = CommandInfo {
            sequence: 1,
            command_type: "ScheduleTask".to_string(),
            timestamp: SystemTime::now(),
            details: CommandDetails::ScheduleTask {
                task_id: "task-1".to_string(),
                task_type: "my_task".to_string(),
                task_queue: "default".to_string(),
            },
        };

        let desc = info.description();
        assert!(desc.contains("my_task"));
        assert!(desc.contains("task-1"));
    }

    #[test]
    fn test_command_info_is_task() {
        let info = CommandInfo {
            sequence: 1,
            command_type: "ScheduleTask".to_string(),
            timestamp: SystemTime::now(),
            details: CommandDetails::ScheduleTask {
                task_id: "task-1".to_string(),
                task_type: "my_task".to_string(),
                task_queue: "default".to_string(),
            },
        };

        assert!(info.is_task());
        assert!(!info.is_timer());
        assert!(!info.is_child_workflow());
        assert!(!info.is_terminal());
    }

    #[test]
    fn test_command_info_is_timer() {
        let info = CommandInfo {
            sequence: 1,
            command_type: "StartTimer".to_string(),
            timestamp: SystemTime::now(),
            details: CommandDetails::StartTimer {
                timer_id: "timer-1".to_string(),
                duration_secs: 60,
            },
        };

        assert!(!info.is_task());
        assert!(info.is_timer());
        assert!(!info.is_child_workflow());
        assert!(!info.is_terminal());
    }

    #[test]
    fn test_command_summary_default() {
        let summary = CommandSummary::default();
        assert_eq!(summary.total_commands, 0);
        assert_eq!(summary.task_count, 0);
        assert!(!summary.completed);
        assert!(!summary.failed);
        assert!(summary.is_running());
    }

    #[test]
    fn test_command_summary_to_string() {
        let summary = CommandSummary {
            total_commands: 5,
            task_count: 2,
            timer_count: 1,
            child_workflow_count: 1,
            event_count: 1,
            wait_event_count: 0,
            start_refresh: false,
            completed: false,
            failed: false,
        };

        let s = summary.to_string();
        assert!(s.contains("5 total"));
        assert!(s.contains("2 tasks"));
        assert!(s.contains("1 timers"));
    }
}
