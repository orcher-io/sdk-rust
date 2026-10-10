//! Attribute types and validation for the workflow, task, query, update and event macros.
//!
//! Most attribute structs are parsed with darling. `WorkflowAttrs` has a hand-written parser
//! that accepts a fixed set of keys and ignores any others.

use darling::FromMeta;
use syn::{punctuated::Punctuated, spanned::Spanned, Expr, Lit, Meta, Token};

/// An attribute that is accepted but has no effect, with where it was written, so the macro
/// can warn about it.
#[derive(Debug, Clone)]
pub struct IneffectiveAttr {
    /// The attribute key as written.
    pub key: &'static str,
    /// What to do instead.
    pub note: &'static str,
    /// Where the key was written.
    pub span: proc_macro2::Span,
}

/// `#[workflow]` attributes that have no effect, with what to do instead.
const WORKFLOW_INEFFECTIVE: &[(&str, &str)] = &[
    (
        "timeout",
        "set the workflow's timeout when you start it, with \
         StartWorkflowOptions::with_workflow_execution_timeout",
    ),
    (
        "version",
        "to tie executions to a code release, set the worker's version with \
         WorkerBuilder::version_id or ORCHER_VERSION_ID",
    ),
    ("description", "describe the workflow in a doc comment"),
    (
        "task_queue",
        "a workflow runs on the task queue it is started on; set it with \
         StartWorkflowOptions::with_task_queue and serve it with WorkerBuilder::task_queue",
    ),
    (
        "max_concurrent",
        "limit how many workflows a worker runs at once with \
         WorkerBuilder::max_concurrent_workflows",
    ),
    ("tags", "nothing reads the tags"),
    (
        "retry_policy",
        "set the workflow's retry policy when you start it, with \
         StartWorkflowOptions::with_retry_policy",
    ),
    (
        "enabled",
        "a registered workflow runs whenever it is started",
    ),
    (
        "cron",
        "run the workflow on a schedule by starting it with \
         StartWorkflowOptions::with_cron_schedule",
    ),
    (
        "schedule",
        "run the workflow on a schedule by starting it with \
         StartWorkflowOptions::with_cron_schedule",
    ),
];

/// `#[task]` and `#[tasks]` attributes that have no effect, with what to do instead.
const TASK_INEFFECTIVE: &[(&str, &str)] = &[
    ("description", "describe the task in a doc comment"),
    (
        "version",
        "to tie executions to a code release, set the worker's version with \
         WorkerBuilder::version_id or ORCHER_VERSION_ID",
    ),
    (
        "task_queue",
        "a task runs on the task queue of the workflow that schedules it, so serve that \
         queue with WorkerBuilder::task_queue",
    ),
    ("priority", "tasks have no priority"),
    (
        "max_concurrent",
        "limit how many tasks a worker runs at once with WorkerBuilder::max_concurrent_tasks",
    ),
    ("rate_limit", "tasks are not rate limited"),
    (
        "resources",
        "tasks are not placed by resources, so size the worker that runs them",
    ),
    (
        "memory",
        "tasks are not placed by resources, so size the worker that runs them",
    ),
    (
        "idempotency_key",
        "a scheduled task is identified by its task id; set a stable one with \
         TaskOptions::with_task_id",
    ),
    (
        "condition",
        "decide in the workflow whether to schedule the task",
    ),
    (
        "parallel",
        "run tasks in parallel by awaiting them together in the workflow",
    ),
    (
        "requires_approval",
        "wait for an approval in the workflow, with WorkflowContext::wait_for_event",
    ),
    (
        "approval_timeout",
        "wait for an approval in the workflow, with WorkflowContext::wait_for_event",
    ),
    (
        "approvers",
        "wait for an approval in the workflow, with WorkflowContext::wait_for_event",
    ),
];

/// Looks `key` up in `table`, returning the ineffective attribute written at `span`.
fn ineffective_in(
    table: &'static [(&'static str, &'static str)],
    key: &str,
    span: proc_macro2::Span,
) -> Option<IneffectiveAttr> {
    table
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(key, note)| IneffectiveAttr { key, note, span })
}

/// The error for `namespace` on `#[task]` or `#[tasks]`: a task belongs to no namespace of
/// its own.
fn task_namespace_error(item: &str, path: &syn::Path) -> darling::Error {
    darling::Error::custom(format!(
        "`namespace` is not a #[{item}] option: a task runs in whatever namespace its \
         worker serves. Set it on the worker (WorkerBuilder::namespace)"
    ))
    .with_span(path)
}

/// Reads the keys of `#[task(...)]` or `#[tasks(...)]`: rejects `namespace` and collects
/// the attributes that have no effect.
fn scan_task_keys(meta: &Meta, item: &str) -> darling::Result<Vec<IneffectiveAttr>> {
    let mut ineffective = Vec::new();
    if let Meta::List(list) = meta {
        for nested in list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
            let path = nested.path();
            let Some(ident) = path.get_ident() else {
                continue;
            };
            let key = ident.to_string();
            if key == "namespace" {
                return Err(task_namespace_error(item, path));
            }
            ineffective.extend(ineffective_in(TASK_INEFFECTIVE, &key, path.span()));
        }
    }
    Ok(ineffective)
}

/// Parses `#[task(...)]`, rejecting `namespace` and recording the attributes that have no
/// effect.
pub fn parse_task_attrs(meta: &Meta) -> darling::Result<TaskAttrs> {
    let ineffective = scan_task_keys(meta, "task")?;
    let mut attrs = TaskAttrs::from_meta(meta)?;
    attrs.ineffective = ineffective;
    Ok(attrs)
}

/// Parses `#[tasks(...)]`, rejecting `namespace` and recording the attributes that have no
/// effect.
pub fn parse_tasks_group_attrs(meta: &Meta) -> darling::Result<TasksGroupAttrs> {
    let ineffective = scan_task_keys(meta, "tasks")?;
    let mut attrs = TasksGroupAttrs::from_meta(meta)?;
    attrs.ineffective = ineffective;
    Ok(attrs)
}

/// Emits a deprecation warning, at the attribute, for each attribute on `#[item]` that is
/// accepted but has no effect.
///
/// A procedural macro cannot emit a warning directly on stable Rust, so this defines a
/// `#[deprecated]` constant carrying the note and uses it with the attribute's span; the
/// compiler then reports the use as a `deprecated` warning. A warning rather than an
/// error keeps code that sets these attributes compiling.
pub fn ineffective_attr_warnings(
    item: &str,
    ineffective: &[IneffectiveAttr],
) -> proc_macro2::TokenStream {
    let warnings = ineffective.iter().map(|attr| {
        let note = format!(
            "`{}` on #[{item}] has no effect and will be removed; {}",
            attr.key, attr.note
        );
        let name = syn::Ident::new(&format!("{}_has_no_effect_on_{item}", attr.key), attr.span);
        quote::quote_spanned! {attr.span=>
            const _: () = {
                #[deprecated(note = #note)]
                #[allow(non_upper_case_globals)]
                const #name: () = ();
                #name
            };
        }
    });
    quote::quote! { #(#warnings)* }
}

/// The defaults a task preset fills in: `"long-running"`, `"quick"` or `"critical"`.
fn task_preset(name: &str) -> Result<TaskAttrs, String> {
    match name {
        "long-running" => Ok(TaskAttrs {
            retry: Some(3),
            timeout_mins: Some(30),
            ..Default::default()
        }),
        // One attempt: `retry` counts attempts, the first included.
        "quick" => Ok(TaskAttrs {
            retry: Some(1),
            timeout_mins: Some(1),
            ..Default::default()
        }),
        "critical" => Ok(TaskAttrs {
            retry: Some(5),
            timeout_mins: Some(60),
            ..Default::default()
        }),
        other => Err(format!(
            "unknown preset `{other}`; use \"long-running\", \"quick\" or \"critical\""
        )),
    }
}

/// Expands the shortcuts of `#[task]`: `preset`, `retry`, `timeout_mins` and `memory`.
///
/// A preset only fills in values that are not set explicitly. `retry = N` becomes an
/// exponential policy (1s initial, 60s max, factor 2.0) unless `retry_policy` is given,
/// `timeout_mins` fills in `timeout`, and `memory` fills in the memory request.
pub fn expand_task_shortcuts(attrs: &mut TaskAttrs) -> Result<(), String> {
    if let Some(preset_name) = attrs.preset.clone() {
        let preset = task_preset(&preset_name)?;
        if attrs.retry.is_none() {
            attrs.retry = preset.retry;
        }
        if attrs.timeout_mins.is_none() {
            attrs.timeout_mins = preset.timeout_mins;
        }
    }

    if let Some(max_attempts) = attrs.retry {
        if attrs.retry_policy.is_none() {
            attrs.retry_policy = Some(RetryPolicyAttr {
                max_attempts,
                initial_interval: 1,
                max_interval: 60,
                backoff_coefficient: 2.0,
            });
        }
    }

    if let Some(timeout_mins) = attrs.timeout_mins {
        if attrs.timeout.is_none() {
            attrs.timeout = Some(timeout_mins * 60);
        }
    }

    if let Some(memory) = &attrs.memory {
        match attrs.resources {
            None => {
                attrs.resources = Some(ResourcesAttr {
                    cpu: None,
                    memory: Some(memory.clone()),
                    disk: None,
                    cpu_limit: None,
                    memory_limit: None,
                    disk_limit: None,
                    network_limit: None,
                });
            }
            Some(ref mut resources) => {
                if resources.memory.is_none() {
                    resources.memory = Some(memory.clone());
                }
            }
        }
    }

    Ok(())
}

/// Retry policy arguments, such as `retry_policy(max_attempts = 5, initial_interval = 2)`.
#[derive(Debug, Clone, FromMeta)]
pub struct RetryPolicyAttr {
    /// Maximum number of attempts, including the first one.
    #[darling(default = "default_max_attempts")]
    pub max_attempts: u32,

    /// Delay before the first retry, in seconds.
    #[darling(default = "default_initial_interval")]
    pub initial_interval: u64,

    /// Upper bound on the delay between retries, in seconds.
    #[darling(default = "default_max_interval")]
    pub max_interval: u64,

    /// Factor applied to the delay after each retry.
    #[darling(default = "default_backoff_coefficient")]
    pub backoff_coefficient: f64,
}

/// Resource requirements, such as `resources(cpu = 2.0, memory = "512Mi")`.
#[derive(Debug, Clone, FromMeta)]
pub struct ResourcesAttr {
    /// CPU cores required.
    #[darling(default)]
    pub cpu: Option<f64>,

    /// Memory required, such as `"512Mi"` or `"1Gi"`.
    #[darling(default)]
    pub memory: Option<String>,

    /// Disk space required, such as `"1Gi"` or `"10Gi"`.
    #[darling(default)]
    pub disk: Option<String>,

    /// CPU limit as a percentage (0-100).
    #[darling(default)]
    #[allow(dead_code)]
    pub cpu_limit: Option<f64>,

    /// Memory limit.
    #[darling(default)]
    #[allow(dead_code)]
    pub memory_limit: Option<String>,

    /// Disk limit.
    #[darling(default)]
    #[allow(dead_code)]
    pub disk_limit: Option<String>,

    /// Network bandwidth limit in bytes per second.
    #[darling(default)]
    #[allow(dead_code)]
    pub network_limit: Option<u64>,
}

/// Arguments accepted by `#[task]`.
#[derive(Debug, Clone, FromMeta, Default)]
pub struct TaskAttrs {
    /// Task name; defaults to the function name.
    #[darling(default)]
    pub name: Option<String>,

    /// Task description. Has no effect; accepted so the macro can warn about it.
    #[darling(default)]
    #[allow(dead_code)]
    pub description: Option<String>,

    /// Task version.
    #[darling(default)]
    pub version: Option<String>,

    /// Task queue used to route executions.
    #[darling(default)]
    pub task_queue: Option<String>,

    /// Full retry policy; takes precedence over `retry`.
    #[darling(default)]
    pub retry_policy: Option<RetryPolicyAttr>,

    /// Task timeout in seconds.
    #[darling(default)]
    pub timeout: Option<u64>,

    /// Seconds without a heartbeat after which the task is considered stalled.
    #[darling(default)]
    pub heartbeat_timeout: Option<u64>,

    /// Resource requirements.
    #[darling(default)]
    pub resources: Option<ResourcesAttr>,

    /// Task priority (0-100, higher is more important).
    #[darling(default)]
    pub priority: Option<u8>,

    /// Maximum number of concurrent executions of this task.
    #[darling(default)]
    pub max_concurrent: Option<u32>,

    /// Maximum executions per second.
    #[darling(default)]
    pub rate_limit: Option<u32>,

    /// Template for an idempotency key. Has no effect; accepted so the macro can warn about
    /// it.
    #[darling(default)]
    #[allow(dead_code)]
    pub idempotency_key: Option<String>,

    /// Condition expression that gates execution.
    #[darling(default)]
    pub condition: Option<String>,

    /// Whether the task may run in parallel.
    #[darling(default)]
    pub parallel: bool,

    /// Whether the task requires human approval before it runs.
    #[darling(default)]
    pub requires_approval: bool,

    /// Approval timeout in seconds.
    #[darling(default)]
    pub approval_timeout: Option<u64>,

    /// Comma-separated approver roles.
    #[darling(default)]
    pub approvers: Option<String>,

    /// Retry up to this many attempts with exponential backoff, as in `#[task(retry = 3)]`.
    /// Ignored when `retry_policy` is set.
    #[darling(default)]
    pub retry: Option<u32>,

    /// Error types the task is never retried on, matched against the type a failure is
    /// reported under (see `TaskError::application`). For example,
    /// `#[task(retry = 3, non_retryable_errors = ["CardDeclined"])]`.
    #[darling(default)]
    pub non_retryable_errors: Vec<syn::LitStr>,

    /// Timeout in minutes, as in `#[task(timeout_mins = 5)]`. Ignored when `timeout` is set.
    #[darling(default)]
    pub timeout_mins: Option<u64>,

    /// Memory request, as in `#[task(memory = "512Mi")]`. Fills in `resources.memory` when
    /// that is not set.
    #[darling(default)]
    pub memory: Option<String>,

    /// Named bundle of defaults: `"long-running"`, `"quick"` or `"critical"`, as in
    /// `#[task(preset = "long-running")]`. Explicit arguments override the preset.
    #[darling(default)]
    pub preset: Option<String>,

    /// Attributes that were set but have no effect, filled in by [`parse_task_attrs`].
    #[darling(skip)]
    pub ineffective: Vec<IneffectiveAttr>,
}

/// Arguments accepted by `#[tasks]`: defaults for every `#[task]` method in the impl block.
///
/// A method's own `#[task(...)]` arguments override these defaults.
#[derive(Debug, Clone, FromMeta, Default)]
pub struct TasksGroupAttrs {
    /// Default retry attempts.
    #[darling(default)]
    pub retry: Option<u32>,

    /// Default timeout in seconds.
    #[darling(default)]
    pub timeout: Option<u64>,

    /// Default timeout in minutes.
    #[darling(default)]
    pub timeout_mins: Option<u64>,

    /// Default memory request.
    #[darling(default)]
    pub memory: Option<String>,

    /// Default task queue.
    #[darling(default)]
    pub task_queue: Option<String>,

    /// Default version.
    #[darling(default)]
    pub version: Option<String>,

    /// Default preset.
    #[darling(default)]
    pub preset: Option<String>,

    /// Default heartbeat timeout in seconds.
    #[darling(default)]
    pub heartbeat_timeout: Option<u64>,

    /// Default priority (0-100).
    #[darling(default)]
    pub priority: Option<u8>,

    /// Default maximum concurrent executions.
    #[darling(default)]
    pub max_concurrent: Option<u32>,

    /// Default rate limit in executions per second.
    #[darling(default)]
    pub rate_limit: Option<u32>,

    /// Attributes that were set but have no effect, filled in by
    /// [`parse_tasks_group_attrs`].
    #[darling(skip)]
    pub ineffective: Vec<IneffectiveAttr>,
}

impl TasksGroupAttrs {
    /// Fills every unset field of `method_attrs` from the group defaults. Values set on the
    /// method are never overwritten.
    pub fn merge_into(&self, method_attrs: &mut TaskAttrs) {
        if method_attrs.retry.is_none() {
            method_attrs.retry = self.retry;
        }
        if method_attrs.timeout.is_none() {
            method_attrs.timeout = self.timeout;
        }
        if method_attrs.timeout_mins.is_none() {
            method_attrs.timeout_mins = self.timeout_mins;
        }
        if method_attrs.memory.is_none() {
            method_attrs.memory = self.memory.clone();
        }
        if method_attrs.task_queue.is_none() {
            method_attrs.task_queue = self.task_queue.clone();
        }
        if method_attrs.version.is_none() {
            method_attrs.version = self.version.clone();
        }
        if method_attrs.preset.is_none() {
            method_attrs.preset = self.preset.clone();
        }
        if method_attrs.heartbeat_timeout.is_none() {
            method_attrs.heartbeat_timeout = self.heartbeat_timeout;
        }
        if method_attrs.priority.is_none() {
            method_attrs.priority = self.priority;
        }
        if method_attrs.max_concurrent.is_none() {
            method_attrs.max_concurrent = self.max_concurrent;
        }
        if method_attrs.rate_limit.is_none() {
            method_attrs.rate_limit = self.rate_limit;
        }
    }
}

/// Arguments accepted by `#[query]`.
#[derive(Debug, Clone, FromMeta)]
pub struct QueryAttrs {
    /// Query name; defaults to the function name.
    #[darling(default)]
    pub name: Option<String>,

    /// Query description.
    #[darling(default)]
    pub description: Option<String>,

    /// Query timeout in seconds.
    #[darling(default)]
    pub timeout: Option<u64>,

    /// Namespace the query belongs to.
    #[darling(default)]
    pub namespace: Option<String>,

    /// How long a result may be cached, in seconds. Must be greater than 0 when set.
    #[darling(default)]
    pub cache_ttl: Option<u64>,

    /// Must be `true`; validation rejects `readonly = false`.
    #[darling(default = "default_readonly")]
    pub readonly: bool,

    /// Query version.
    #[darling(default)]
    pub version: Option<String>,
}

/// Arguments accepted by `#[update]`.
#[derive(Debug, Clone, Default, FromMeta)]
pub struct UpdateAttrs {
    /// Update handler name; defaults to the function name.
    #[darling(default)]
    pub name: Option<String>,

    /// Update handler description.
    #[darling(default)]
    pub description: Option<String>,

    /// Update handler timeout in seconds.
    #[darling(default)]
    pub timeout: Option<u64>,

    /// Namespace the update handler belongs to.
    #[darling(default)]
    pub namespace: Option<String>,

    /// Update handler version.
    #[darling(default)]
    pub version: Option<String>,
}

/// Arguments accepted by `#[event]`.
#[derive(Debug, Clone, FromMeta)]
pub struct EventAttrs {
    /// Event name; defaults to the function name.
    #[darling(default)]
    pub name: Option<String>,

    /// Event description.
    #[darling(default)]
    pub description: Option<String>,

    /// Event handler timeout in seconds.
    #[darling(default)]
    pub timeout: Option<u64>,

    /// Namespace the event belongs to.
    #[darling(default)]
    pub namespace: Option<String>,

    /// Event version.
    #[darling(default)]
    pub version: Option<String>,

    /// Maximum number of queued events (1000 by default).
    #[darling(default)]
    pub max_queue_size: Option<u32>,

    /// Event priority (0-100, higher is more important).
    #[darling(default)]
    pub priority: Option<u8>,
}

/// Arguments accepted by `#[workflow]`.
///
/// The parser reads `name`, `description`, `version`, `task_queue`, `timeout`,
/// `max_concurrent`, `enabled`, `cron`, `schedule`, `tags(...)` and `retry_policy(...)`.
/// Other keys are ignored, so the remaining fields always keep their defaults. Every key
/// but `name` has no effect and is recorded in `ineffective`, so the macro can warn about
/// it. `namespace` is rejected: the namespace belongs to the worker and the client.
#[derive(Debug, Clone)]
pub struct WorkflowAttrs {
    /// Workflow name; defaults to the function name.
    pub name: Option<String>,

    /// Workflow description.
    pub description: Option<String>,

    /// Workflow version ("1.0.0" by default). Has no effect; see `ineffective`.
    pub version: String,

    /// Task queue used to route workflow executions.
    pub task_queue: Option<String>,

    /// Workflow timeout in seconds. Has no effect; see `ineffective`.
    pub timeout: Option<u64>,

    /// Maximum number of concurrent steps (10 by default).
    pub max_concurrent: usize,

    /// Maximum number of concurrent executions of this workflow.
    pub max_concurrent_executions: Option<u32>,

    /// Workflow tags.
    pub tags: Vec<String>,

    /// Retry policy for the whole workflow.
    pub retry_policy: Option<RetryPolicyAttr>,

    /// Cron expression for running the workflow periodically.
    pub cron: Option<String>,

    /// Alias for `cron`; `cron` takes precedence when both are set.
    pub schedule: Option<String>,

    /// Whether the workflow is enabled (`true` by default).
    pub enabled: bool,

    /// Isolation level: "none", "thread", "process" or "container".
    #[allow(dead_code)]
    pub isolation: Option<String>,

    /// Resource requirements.
    #[allow(dead_code)]
    pub resources: Option<ResourcesAttr>,

    /// Whether to send heartbeats automatically.
    pub auto_heartbeat: bool,

    /// Heartbeat interval in seconds, used with `auto_heartbeat`.
    pub heartbeat_interval: Option<u64>,

    /// Behavior on timeout: "cancel", "cancel_with_notification" or "fail".
    pub on_timeout: Option<String>,

    /// Maximum number of attempts for the whole workflow.
    pub max_retry_attempts: Option<u32>,

    /// Attributes that were set but have no effect, with where each was written.
    pub ineffective: Vec<IneffectiveAttr>,
}

// Defaults referenced by the darling attributes above.
fn default_max_attempts() -> u32 {
    3
}

fn default_initial_interval() -> u64 {
    1
}

fn default_max_interval() -> u64 {
    60
}

fn default_backoff_coefficient() -> f64 {
    2.0
}

fn default_version() -> String {
    "1.0.0".to_string()
}

fn default_max_concurrent() -> usize {
    10
}

fn default_enabled() -> bool {
    true
}

fn default_readonly() -> bool {
    true
}

fn default_max_queue_size() -> u32 {
    1000
}

impl Default for RetryPolicyAttr {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_interval: default_initial_interval(),
            max_interval: default_max_interval(),
            backoff_coefficient: default_backoff_coefficient(),
        }
    }
}

impl Default for QueryAttrs {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            timeout: None,
            namespace: None,
            cache_ttl: None,
            readonly: true,
            version: None,
        }
    }
}

// `UpdateAttrs` derives `Default`: every field is optional.

impl Default for EventAttrs {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            timeout: None,
            namespace: None,
            version: None,
            max_queue_size: Some(default_max_queue_size()),
            priority: None,
        }
    }
}

impl Default for WorkflowAttrs {
    fn default() -> Self {
        Self {
            name: None,
            description: None,
            version: default_version(),
            task_queue: None,
            timeout: None,
            max_concurrent: default_max_concurrent(),
            max_concurrent_executions: None,
            tags: Vec::new(),
            retry_policy: None,
            cron: None,
            schedule: None,
            enabled: default_enabled(),
            isolation: None,
            resources: None,
            auto_heartbeat: false,
            heartbeat_interval: None,
            on_timeout: None,
            max_retry_attempts: None,
            ineffective: Vec::new(),
        }
    }
}

impl FromMeta for WorkflowAttrs {
    fn from_meta(meta: &Meta) -> darling::Result<Self> {
        let mut attrs = WorkflowAttrs::default();

        if let Meta::List(list) = meta {
            for nested in list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
                if let Some(ident) = nested.path().get_ident() {
                    attrs.ineffective.extend(ineffective_in(
                        WORKFLOW_INEFFECTIVE,
                        &ident.to_string(),
                        nested.path().span(),
                    ));
                }
                match nested {
                    Meta::NameValue(nv) => {
                        let ident = nv.path.get_ident().ok_or_else(|| {
                            darling::Error::custom("expected identifier").with_span(&nv.path)
                        })?;

                        match ident.to_string().as_str() {
                            "name" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.name = Some(s.value());
                                    }
                                }
                            }
                            "description" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.description = Some(s.value());
                                    }
                                }
                            }
                            "version" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.version = s.value();
                                    }
                                }
                            }
                            "namespace" => {
                                return Err(darling::Error::custom(
                                    "`namespace` is not a #[workflow] option: a workflow runs in \
                                     whatever namespace its worker serves. Set it on the worker \
                                     (WorkerBuilder::namespace) and where you start workflows \
                                     (ClientConfig::with_namespace, \
                                     StartWorkflowOptions::with_namespace)",
                                )
                                .with_span(&nv.path));
                            }
                            "timeout" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Int(i) = &lit.lit {
                                        attrs.timeout = Some(i.base10_parse()?);
                                    }
                                }
                            }
                            "max_concurrent" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Int(i) = &lit.lit {
                                        attrs.max_concurrent = i.base10_parse()?;
                                    }
                                }
                            }
                            "enabled" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Bool(b) = &lit.lit {
                                        attrs.enabled = b.value();
                                    }
                                }
                            }
                            "cron" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.cron = Some(s.value());
                                    }
                                }
                            }
                            "schedule" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.schedule = Some(s.value());
                                    }
                                }
                            }
                            "task_queue" => {
                                if let Expr::Lit(lit) = &nv.value {
                                    if let Lit::Str(s) = &lit.lit {
                                        attrs.task_queue = Some(s.value());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Meta::List(list) => {
                        let ident = list.path.get_ident().ok_or_else(|| {
                            darling::Error::custom("expected identifier").with_span(&list.path)
                        })?;

                        if ident == "tags" {
                            for tag_meta in list
                                .parse_args_with(Punctuated::<Lit, Token![,]>::parse_terminated)?
                            {
                                if let Lit::Str(s) = tag_meta {
                                    attrs.tags.push(s.value());
                                }
                            }
                        } else if ident == "retry_policy" {
                            attrs.retry_policy =
                                Some(RetryPolicyAttr::from_meta(&Meta::List(list))?);
                        } else if ident == "schedule" {
                            if let Ok(Lit::Str(s)) = list.parse_args::<Lit>() {
                                attrs.schedule = Some(s.value());
                            }
                        } else if ident == "cron" {
                            if let Ok(Lit::Str(s)) = list.parse_args::<Lit>() {
                                attrs.cron = Some(s.value());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(attrs)
    }
}

/// Checks that a retry policy has at least one attempt, a non-zero initial interval, and a
/// maximum interval no smaller than the initial one.
pub fn validate_retry_policy(policy: &RetryPolicyAttr) -> Result<(), String> {
    if policy.max_attempts == 0 {
        return Err("max_attempts must be greater than 0".to_string());
    }

    if policy.initial_interval == 0 {
        return Err("initial_interval must be greater than 0".to_string());
    }

    if policy.max_interval < policy.initial_interval {
        return Err("max_interval must be greater than or equal to initial_interval".to_string());
    }

    Ok(())
}

/// Checks that the CPU count is positive and that memory and disk sizes are well formed.
pub fn validate_resources(resources: &ResourcesAttr) -> Result<(), String> {
    if let Some(cpu) = resources.cpu {
        if cpu <= 0.0 {
            return Err("CPU must be greater than 0".to_string());
        }
    }

    if let Some(ref memory) = resources.memory {
        if !is_valid_memory_spec(memory) {
            return Err(format!("Invalid memory specification: {}", memory));
        }
    }

    if let Some(ref disk) = resources.disk {
        if !is_valid_memory_spec(disk) {
            return Err(format!("Invalid disk specification: {}", disk));
        }
    }

    Ok(())
}

/// Checks that a condition expression is non-empty and has balanced braces.
///
/// The expression itself is not parsed.
pub fn validate_condition(condition: &str) -> Result<(), String> {
    if condition.is_empty() {
        return Err("Condition expression cannot be empty".to_string());
    }

    let open_count = condition.chars().filter(|c| *c == '{').count();
    let close_count = condition.chars().filter(|c| *c == '}').count();

    if open_count != close_count {
        return Err("Unbalanced braces in condition expression".to_string());
    }

    Ok(())
}

/// Checks that a cron expression has 5 or 6 whitespace-separated fields.
///
/// The fields themselves are not validated.
pub fn validate_schedule(schedule: &str) -> Result<(), String> {
    if schedule.is_empty() {
        return Err("Schedule expression cannot be empty".to_string());
    }

    let parts: Vec<&str> = schedule.split_whitespace().collect();
    if parts.len() < 5 || parts.len() > 6 {
        return Err(format!(
            "Invalid cron expression: expected 5 or 6 fields, got {}",
            parts.len()
        ));
    }

    Ok(())
}

/// Checks the workflow's heartbeat interval, timeout behavior, execution and retry limits,
/// and cron schedule.
pub fn validate_workflow_attrs(attrs: &WorkflowAttrs) -> Result<(), String> {
    if attrs.auto_heartbeat {
        if let Some(interval) = attrs.heartbeat_interval {
            if interval == 0 {
                return Err("Heartbeat interval must be greater than 0".to_string());
            }
        }
    }

    if let Some(ref handler) = attrs.on_timeout {
        match handler.as_str() {
            "cancel" | "cancel_with_notification" | "fail" => {}
            _ => {
                return Err(format!(
                    "Invalid timeout handler '{}': must be 'cancel', 'cancel_with_notification', or 'fail'",
                    handler
                ));
            }
        }
    }

    if let Some(max_exec) = attrs.max_concurrent_executions {
        if max_exec == 0 {
            return Err("max_concurrent_executions must be greater than 0".to_string());
        }
    }

    if let Some(max_retry) = attrs.max_retry_attempts {
        if max_retry == 0 {
            return Err("max_retry_attempts must be greater than 0".to_string());
        }
    }

    // Only the expression that takes effect is checked: `cron` if set, otherwise `schedule`.
    if let Some(ref cron_expr) = attrs.cron {
        validate_schedule(cron_expr)?;
    } else if let Some(ref schedule_expr) = attrs.schedule {
        validate_schedule(schedule_expr)?;
    }

    Ok(())
}

/// Checks that an approval timeout, when approval is required, is greater than 0.
pub fn validate_approval(requires_approval: bool, timeout: Option<u64>) -> Result<(), String> {
    if requires_approval {
        if let Some(t) = timeout {
            if t == 0 {
                return Err("Approval timeout must be greater than 0".to_string());
            }
        }
    }
    Ok(())
}

/// Checks the query's timeout, cache TTL and read-only flag.
pub fn validate_query_attrs(attrs: &QueryAttrs) -> Result<(), String> {
    if let Some(timeout) = attrs.timeout {
        if timeout == 0 {
            return Err("Query timeout must be greater than 0".to_string());
        }
    }

    if let Some(cache_ttl) = attrs.cache_ttl {
        if cache_ttl == 0 {
            return Err("Cache TTL must be greater than 0".to_string());
        }
    }

    if !attrs.readonly {
        return Err("Queries must be read-only (readonly = true)".to_string());
    }

    Ok(())
}

/// Checks the update handler's timeout.
pub fn validate_update_attrs(attrs: &UpdateAttrs) -> Result<(), String> {
    if let Some(timeout) = attrs.timeout {
        if timeout == 0 {
            return Err("Update handler timeout must be greater than 0".to_string());
        }
    }

    Ok(())
}

/// Checks the event's timeout, queue size and priority.
pub fn validate_event_attrs(attrs: &EventAttrs) -> Result<(), String> {
    if let Some(timeout) = attrs.timeout {
        if timeout == 0 {
            return Err("Event timeout must be greater than 0".to_string());
        }
    }

    if let Some(max_queue_size) = attrs.max_queue_size {
        if max_queue_size == 0 {
            return Err("Max queue size must be greater than 0".to_string());
        }
    }

    if let Some(priority) = attrs.priority {
        if priority > 100 {
            return Err("Priority must be between 0 and 100".to_string());
        }
    }

    Ok(())
}

/// Checks the task's heartbeat timeout, priority, concurrency and rate limit.
pub fn validate_task_attrs(attrs: &TaskAttrs) -> Result<(), String> {
    if let Some(heartbeat_timeout) = attrs.heartbeat_timeout {
        if heartbeat_timeout == 0 {
            return Err("Heartbeat timeout must be greater than 0".to_string());
        }

        // A heartbeat timeout only makes sense if it is shorter than the task timeout.
        if let Some(timeout) = attrs.timeout {
            if heartbeat_timeout >= timeout {
                return Err("Heartbeat timeout must be less than task timeout".to_string());
            }
        }
    }

    if let Some(priority) = attrs.priority {
        if priority > 100 {
            return Err("Priority must be between 0 and 100".to_string());
        }
    }

    if let Some(max_concurrent) = attrs.max_concurrent {
        if max_concurrent == 0 {
            return Err("max_concurrent must be greater than 0".to_string());
        }
    }

    if let Some(rate_limit) = attrs.rate_limit {
        if rate_limit == 0 {
            return Err("rate_limit must be greater than 0".to_string());
        }
    }

    Ok(())
}

/// Returns whether `spec` is a size such as `512Mi`, `1Gi`, `2.5G` or a plain byte count.
fn is_valid_memory_spec(spec: &str) -> bool {
    let valid_suffixes = ["Ki", "Mi", "Gi", "Ti", "K", "M", "G", "T"];

    if spec.is_empty() {
        return false;
    }

    for suffix in &valid_suffixes {
        if let Some(number_part) = spec.strip_suffix(suffix) {
            return number_part.parse::<f64>().is_ok();
        }
    }

    // Without a suffix, the value must be a whole number of bytes.
    spec.parse::<u64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicyAttr::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_interval, 1);
        assert_eq!(policy.max_interval, 60);
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_validate_retry_policy_success() {
        let policy = RetryPolicyAttr::default();
        assert!(validate_retry_policy(&policy).is_ok());
    }

    #[test]
    fn test_validate_retry_policy_zero_attempts() {
        let policy = RetryPolicyAttr {
            max_attempts: 0,
            ..Default::default()
        };
        assert!(validate_retry_policy(&policy).is_err());
    }

    #[test]
    fn test_validate_retry_policy_invalid_delays() {
        let policy = RetryPolicyAttr {
            initial_interval: 60,
            max_interval: 10,
            ..Default::default()
        };
        assert!(validate_retry_policy(&policy).is_err());
    }

    #[test]
    fn test_is_valid_memory_spec() {
        assert!(is_valid_memory_spec("512Mi"));
        assert!(is_valid_memory_spec("1Gi"));
        assert!(is_valid_memory_spec("100M"));
        assert!(is_valid_memory_spec("2.5Gi"));
        assert!(is_valid_memory_spec("1024"));
        assert!(!is_valid_memory_spec(""));
        assert!(!is_valid_memory_spec("invalid"));
        assert!(!is_valid_memory_spec("Mi"));
    }

    #[test]
    fn test_validate_resources_cpu() {
        let resources = ResourcesAttr {
            cpu: Some(-1.0),
            memory: None,
            disk: None,
            cpu_limit: Some(-1.0),
            memory_limit: None,
            disk_limit: None,
            network_limit: None,
        };
        assert!(validate_resources(&resources).is_err());

        let resources = ResourcesAttr {
            cpu: Some(1.5),
            memory: None,
            disk: None,
            cpu_limit: Some(1.5),
            memory_limit: None,
            disk_limit: None,
            network_limit: None,
        };
        assert!(validate_resources(&resources).is_ok());
    }

    #[test]
    fn test_validate_resources_memory() {
        let resources = ResourcesAttr {
            cpu: None,
            memory: Some("invalid".to_string()),
            disk: None,
            cpu_limit: None,
            memory_limit: Some("invalid".to_string()),
            disk_limit: None,
            network_limit: None,
        };
        assert!(validate_resources(&resources).is_err());

        let resources = ResourcesAttr {
            cpu: None,
            memory: Some("512Mi".to_string()),
            disk: None,
            cpu_limit: None,
            memory_limit: Some("512Mi".to_string()),
            disk_limit: None,
            network_limit: None,
        };
        assert!(validate_resources(&resources).is_ok());
    }

    #[test]
    fn test_workflow_attrs_defaults() {
        let attrs = WorkflowAttrs::default();
        assert_eq!(attrs.version, "1.0.0");
        assert_eq!(attrs.max_concurrent, 10);
        assert!(attrs.name.is_none());
        assert!(attrs.tags.is_empty());
        assert!(attrs.enabled);
    }

    #[test]
    fn test_validate_condition_success() {
        assert!(validate_condition("${task.status} == 'success'").is_ok());
        assert!(validate_condition("${var1} > 10").is_ok());
    }

    #[test]
    fn test_validate_condition_empty() {
        assert!(validate_condition("").is_err());
    }

    #[test]
    fn test_validate_condition_unbalanced_braces() {
        assert!(validate_condition("${var1").is_err());
        assert!(validate_condition("var1}").is_err());
    }

    #[test]
    fn test_validate_schedule_success() {
        assert!(validate_schedule("0 0 * * *").is_ok());
        assert!(validate_schedule("0 0 0 * * *").is_ok());
    }

    #[test]
    fn test_validate_schedule_invalid() {
        assert!(validate_schedule("").is_err());
        assert!(validate_schedule("0 0").is_err());
        assert!(validate_schedule("0 0 * * * * *").is_err());
    }

    #[test]
    fn test_validate_approval_success() {
        assert!(validate_approval(true, Some(300)).is_ok());
        assert!(validate_approval(false, None).is_ok());
    }

    #[test]
    fn test_validate_approval_zero_timeout() {
        assert!(validate_approval(true, Some(0)).is_err());
    }

    #[test]
    fn test_validate_workflow_attrs_heartbeat() {
        use super::WorkflowAttrs;

        // Valid heartbeat configuration
        let mut attrs = WorkflowAttrs {
            auto_heartbeat: true,
            heartbeat_interval: Some(30),
            ..Default::default()
        };
        assert!(validate_workflow_attrs(&attrs).is_ok());

        // Invalid: zero heartbeat interval
        attrs.heartbeat_interval = Some(0);
        assert!(validate_workflow_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_workflow_attrs_timeout_handler() {
        use super::WorkflowAttrs;

        // Valid timeout handlers
        let mut attrs = WorkflowAttrs {
            on_timeout: Some("cancel".to_string()),
            ..Default::default()
        };
        assert!(validate_workflow_attrs(&attrs).is_ok());

        attrs.on_timeout = Some("cancel_with_notification".to_string());
        assert!(validate_workflow_attrs(&attrs).is_ok());

        attrs.on_timeout = Some("fail".to_string());
        assert!(validate_workflow_attrs(&attrs).is_ok());

        // Invalid timeout handler
        attrs.on_timeout = Some("invalid".to_string());
        assert!(validate_workflow_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_workflow_attrs_max_concurrent() {
        use super::WorkflowAttrs;

        // Valid max concurrent executions
        let mut attrs = WorkflowAttrs {
            max_concurrent_executions: Some(5),
            ..Default::default()
        };
        assert!(validate_workflow_attrs(&attrs).is_ok());

        // Invalid: zero max concurrent executions
        attrs.max_concurrent_executions = Some(0);
        assert!(validate_workflow_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_workflow_attrs_cron() {
        use super::WorkflowAttrs;

        // Valid cron expression
        let mut attrs = WorkflowAttrs {
            cron: Some("0 0 * * *".to_string()),
            ..Default::default()
        };
        assert!(validate_workflow_attrs(&attrs).is_ok());

        // Invalid cron expression
        attrs.cron = Some("invalid".to_string());
        assert!(validate_workflow_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_task_attrs_heartbeat() {
        use super::TaskAttrs;

        // Valid heartbeat timeout
        let mut attrs = TaskAttrs {
            heartbeat_timeout: Some(30),
            timeout: Some(60),
            ..Default::default()
        };
        assert!(validate_task_attrs(&attrs).is_ok());

        // Invalid: heartbeat >= timeout
        attrs.heartbeat_timeout = Some(60);
        assert!(validate_task_attrs(&attrs).is_err());

        // Invalid: zero heartbeat timeout
        attrs.heartbeat_timeout = Some(0);
        assert!(validate_task_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_task_attrs_priority() {
        use super::TaskAttrs;

        // Valid priority
        let mut attrs = TaskAttrs {
            priority: Some(50),
            ..Default::default()
        };
        assert!(validate_task_attrs(&attrs).is_ok());

        // Valid: maximum priority
        attrs.priority = Some(100);
        assert!(validate_task_attrs(&attrs).is_ok());

        // Invalid: priority > 100
        attrs.priority = Some(101);
        assert!(validate_task_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_task_attrs_concurrency() {
        use super::TaskAttrs;

        // Valid max concurrent
        let mut attrs = TaskAttrs {
            max_concurrent: Some(10),
            ..Default::default()
        };
        assert!(validate_task_attrs(&attrs).is_ok());

        // Invalid: zero max concurrent
        attrs.max_concurrent = Some(0);
        assert!(validate_task_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_task_attrs_rate_limit() {
        use super::TaskAttrs;

        // Valid rate limit
        let mut attrs = TaskAttrs {
            rate_limit: Some(100),
            ..Default::default()
        };
        assert!(validate_task_attrs(&attrs).is_ok());

        // Invalid: zero rate limit
        attrs.rate_limit = Some(0);
        assert!(validate_task_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_query_attrs_timeout() {
        use super::QueryAttrs;

        // Valid timeout
        let mut attrs = QueryAttrs {
            timeout: Some(30),
            ..Default::default()
        };
        assert!(validate_query_attrs(&attrs).is_ok());

        // Invalid: zero timeout
        attrs.timeout = Some(0);
        assert!(validate_query_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_query_attrs_cache_ttl() {
        use super::QueryAttrs;

        // Valid cache TTL
        let mut attrs = QueryAttrs {
            cache_ttl: Some(300),
            ..Default::default()
        };
        assert!(validate_query_attrs(&attrs).is_ok());

        // Invalid: zero cache TTL
        attrs.cache_ttl = Some(0);
        assert!(validate_query_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_query_attrs_readonly() {
        use super::QueryAttrs;

        // Valid: readonly = true (default)
        let attrs = QueryAttrs::default();
        assert!(attrs.readonly);
        assert!(validate_query_attrs(&attrs).is_ok());

        // Invalid: readonly = false
        let attrs = QueryAttrs {
            readonly: false,
            ..Default::default()
        };
        assert!(validate_query_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_event_attrs_timeout() {
        use super::EventAttrs;

        // Valid timeout
        let mut attrs = EventAttrs {
            timeout: Some(30),
            ..Default::default()
        };
        assert!(validate_event_attrs(&attrs).is_ok());

        // Invalid: zero timeout
        attrs.timeout = Some(0);
        assert!(validate_event_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_event_attrs_queue_size() {
        use super::EventAttrs;

        // Valid queue size
        let mut attrs = EventAttrs {
            max_queue_size: Some(500),
            ..Default::default()
        };
        assert!(validate_event_attrs(&attrs).is_ok());

        // Invalid: zero queue size
        attrs.max_queue_size = Some(0);
        assert!(validate_event_attrs(&attrs).is_err());
    }

    #[test]
    fn test_validate_event_attrs_priority() {
        use super::EventAttrs;

        // Valid priority
        let mut attrs = EventAttrs {
            priority: Some(50),
            ..Default::default()
        };
        assert!(validate_event_attrs(&attrs).is_ok());

        // Valid: max priority
        attrs.priority = Some(100);
        assert!(validate_event_attrs(&attrs).is_ok());

        // Invalid: priority > 100
        attrs.priority = Some(101);
        assert!(validate_event_attrs(&attrs).is_err());
    }

    #[test]
    fn test_tasks_group_attrs_merge_into() {
        let group = TasksGroupAttrs {
            retry: Some(3),
            timeout: Some(60),
            task_queue: Some("payments".to_string()),
            ..Default::default()
        };

        // A method without overrides gets every group default.
        let mut method = TaskAttrs::default();
        group.merge_into(&mut method);
        assert_eq!(method.retry, Some(3));
        assert_eq!(method.timeout, Some(60));
        assert_eq!(method.task_queue, Some("payments".to_string()));

        // A method that sets `retry` keeps it and gets the rest from the group.
        let mut method = TaskAttrs {
            retry: Some(10),
            ..Default::default()
        };
        group.merge_into(&mut method);
        assert_eq!(method.retry, Some(10)); // method wins
        assert_eq!(method.timeout, Some(60)); // group default
        assert_eq!(method.task_queue, Some("payments".to_string())); // group default
    }
}
