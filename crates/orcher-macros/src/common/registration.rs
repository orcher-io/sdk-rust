//! `inventory` registration for workflows and tasks.
//!
//! `#[workflow]` and `#[task]` keep the user's function, generate a payload-level handler,
//! and emit a registration function that inserts the handler into a worker's `Registry`.
//! The registration is collected through `inventory::submit!`, which needs a
//! const-constructible value. The handler is an `Arc` closure and cannot be built in a
//! const context, so only a pointer to the registration function is submitted and the
//! handler is built when the worker runs it.
//!
//! # Example
//!
//! ```text
//! #[workflow]
//! async fn my_workflow(ctx: WorkflowContext, input: Order) -> Result<Receipt> {
//!     // ...
//! }
//! ```
//!
//! Expands to roughly:
//!
//! ```text
//! async fn my_workflow(ctx: WorkflowContext, input: Order) -> Result<Receipt> {
//!     // original body
//! }
//!
//! fn __orcher_register_workflow_my_workflow(registry: &mut orcher::worker::Registry) {
//!     let handler = std::sync::Arc::new(|ctx, input| {
//!         Box::pin(async move { my_workflow_handler(ctx, input).await })
//!     });
//!     registry.workflows.insert("my_workflow".to_string(), WorkflowHandler { ... });
//! }
//!
//! ::orcher::__private::inventory::submit! {
//!     orcher::worker::registration::WorkflowRegistration::new(__orcher_register_workflow_my_workflow)
//! }
//! ```

use crate::common::attrs::{TaskAttrs, WorkflowAttrs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

/// Parses a memory size such as `512M`, `512MB`, `512Mi`, `1G` or `1024K` into bytes.
///
/// Units are case-insensitive and binary (`M` and `Mi` both mean 1024 * 1024). A number
/// without a unit is taken as bytes; an unparsable number yields 0.
#[allow(dead_code)]
fn parse_memory_size(size_str: &str) -> u64 {
    let size_str = size_str.trim().to_uppercase();

    let (num_str, unit) = if size_str.ends_with("GIB") || size_str.ends_with("GI") {
        let num = size_str.trim_end_matches("GIB").trim_end_matches("GI");
        (num, 1024u64 * 1024 * 1024)
    } else if size_str.ends_with("GB") || size_str.ends_with("G") {
        let num = size_str.trim_end_matches("GB").trim_end_matches("G");
        (num, 1024u64 * 1024 * 1024)
    } else if size_str.ends_with("MIB") || size_str.ends_with("MI") {
        let num = size_str.trim_end_matches("MIB").trim_end_matches("MI");
        (num, 1024u64 * 1024)
    } else if size_str.ends_with("MB") || size_str.ends_with("M") {
        let num = size_str.trim_end_matches("MB").trim_end_matches("M");
        (num, 1024u64 * 1024)
    } else if size_str.ends_with("KIB") || size_str.ends_with("KI") {
        let num = size_str.trim_end_matches("KIB").trim_end_matches("KI");
        (num, 1024u64)
    } else if size_str.ends_with("KB") || size_str.ends_with("K") {
        let num = size_str.trim_end_matches("KB").trim_end_matches("K");
        (num, 1024u64)
    } else {
        (&size_str[..], 1u64)
    };

    num_str.parse::<u64>().unwrap_or(0).saturating_mul(unit)
}

/// Generates the `inventory` registration for a task.
///
/// The registration inserts the task handler into the worker registry and records the
/// task's retry policy and declared limits (`timeout`, `heartbeat_timeout`) in
/// process-global registries. The other task attributes are parsed and validated but not
/// registered.
///
/// # Generated Code
///
/// The shape below is simplified; the actual expansion submits a registration function
/// pointer, as shown in the module documentation.
///
/// ```text
/// #[cfg(feature = "auto-register")]
/// ::orcher::__private::inventory::submit! {
///     orcher::worker::TaskRegistration {
///         name: "my_task",
///         handler: std::sync::Arc::new(|ctx, input| {
///             Box::pin(async move {
///                 my_task_handler(ctx, input).await
///             })
///         }),
///         metadata: Some(orcher::worker::TaskMetadata {
///             description: Some("Process data".to_string()),
///             timeout_seconds: Some(30),
///             retry_policy: Some(orcher::worker::RetryPolicy { /* ... */ }),
///             namespace: Some("default".to_string()),
///             version: Some("1.0.0".to_string()),
///         }),
///     }
/// }
/// ```
pub fn generate_sdk_task_registration(
    fn_name: &Ident,
    attrs: &TaskAttrs,
    handler_wrapper_name: &Ident,
) -> TokenStream {
    let task_name = attrs
        .name
        .as_ref()
        .map(|s| s.to_string())
        .unwrap_or_else(|| fn_name.to_string());

    let _description = attrs
        .description
        .as_ref()
        .map(|d| quote! { Some(#d.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let timeout_seconds = attrs
        .timeout
        .map(|t| quote! { Some(#t) })
        .unwrap_or_else(|| quote! { None });

    let _namespace = attrs
        .namespace
        .as_ref()
        .map(|ns| quote! { Some(#ns.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _version = attrs
        .version
        .as_ref()
        .map(|v| quote! { Some(#v.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _task_queue = attrs
        .task_queue
        .as_ref()
        .map(|tq| quote! { Some(#tq.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let heartbeat_timeout_seconds = attrs
        .heartbeat_timeout
        .map(|ht| quote! { Some(#ht) })
        .unwrap_or_else(|| quote! { None });

    let _priority = attrs
        .priority
        .map(|p| quote! { Some(#p) })
        .unwrap_or_else(|| quote! { None });

    let _max_concurrent = attrs
        .max_concurrent
        .map(|mc| quote! { Some(#mc) })
        .unwrap_or_else(|| quote! { None });

    let _rate_limit = attrs
        .rate_limit
        .map(|rl| quote! { Some(#rl) })
        .unwrap_or_else(|| quote! { None });

    let _idempotency_key = attrs
        .idempotency_key
        .as_ref()
        .map(|ik| quote! { Some(#ik.to_string()) })
        .unwrap_or_else(|| quote! { None });

    // The retry policy goes into a process-global registry keyed by task name. A workflow
    // schedules a task by name, so `execute_task` looks the policy up there and attaches it
    // to the schedule command.
    let non_retryable_errors = &attrs.non_retryable_errors;
    let retry_policy_tokens = if let Some(ref retry_attr) = attrs.retry_policy {
        let max_attempts = retry_attr.max_attempts;
        let initial_interval = retry_attr.initial_interval;
        let max_interval = retry_attr.max_interval;
        let backoff_coefficient = retry_attr.backoff_coefficient;

        // Attribute values are whole seconds, because attribute literals cannot be
        // `Duration`s.
        quote! {
            Some(
                <orcher::task::RetryPolicy as ::core::default::Default>::default()
                    .with_max_attempts(#max_attempts)
                    .with_initial_interval(::std::time::Duration::from_secs(#initial_interval))
                    .with_max_interval(::std::time::Duration::from_secs(#max_interval))
                    .with_backoff_coefficient(#backoff_coefficient)
                    .with_non_retryable_errors(::std::vec::Vec::<&'static str>::from([#(#non_retryable_errors),*])),
            )
        }
    } else if !non_retryable_errors.is_empty() {
        // A non-retryable error list alone keeps the default attempts and backoff.
        quote! {
            Some(
                <orcher::task::RetryPolicy as ::core::default::Default>::default()
                    .with_non_retryable_errors(::std::vec::Vec::<&'static str>::from([#(#non_retryable_errors),*])),
            )
        }
    } else {
        quote! { None }
    };

    let register_fn_name = syn::Ident::new(
        &format!("__orcher_register_task_{}", fn_name),
        fn_name.span(),
    );

    quote! {
        // Builds the handler at run time and inserts it into the worker registry.
        #[doc(hidden)]
        fn #register_fn_name(registry: &mut orcher::worker::Registry) {
            use orcher::worker::registration::*;

            let handler: orcher::worker::registration::TaskHandlerFn = std::sync::Arc::new(|ctx, input| {
                Box::pin(async move {
                    #handler_wrapper_name(ctx, input).await
                }) as orcher::worker::registration::BoxFuture<'static, std::result::Result<orcher::Payload, orcher::Error>>
            });

            let task_handler = orcher::worker::registry::TaskHandler {
                name: #task_name.to_string(),
                handler: Some(handler),
            };

            registry.tasks.insert(#task_name.to_string(), task_handler);

            // Lets `execute_task` attach the task's retry policy when it schedules the task.
            orcher::worker::registration::register_task_retry_policy(
                #task_name.to_string(),
                #retry_policy_tokens,
            );

            // Registers the declared timeouts. Without this, the task would run under the
            // scheduler's defaults instead of the limits it declares.
            orcher::worker::registration::register_task_limits(
                #task_name.to_string(),
                orcher::worker::registration::DeclaredTaskLimits {
                    timeout_secs: #timeout_seconds,
                    heartbeat_timeout_secs: #heartbeat_timeout_seconds,
                },
            );
        }

        // Submit only the function pointer: `inventory::submit!` requires a const value.
        ::orcher::__private::inventory::submit! {
            orcher::worker::registration::TaskRegistration::new(#register_fn_name)
        }
    }
}

/// Generates the `inventory` registration for a workflow.
///
/// Only the workflow name and handler are registered. The other workflow attributes are
/// parsed and validated but not registered.
///
/// # Generated Code
///
/// The shape below is simplified; the actual expansion submits a registration function
/// pointer, as shown in the module documentation.
///
/// ```text
/// #[cfg(feature = "auto-register")]
/// ::orcher::__private::inventory::submit! {
///     orcher::worker::WorkflowRegistration {
///         name: "my_workflow",
///         handler: std::sync::Arc::new(|ctx, input| {
///             Box::pin(async move {
///                 my_workflow_handler(ctx, input).await
///             })
///         }),
///         metadata: Some(orcher::worker::WorkflowMetadata {
///             description: Some("Process orders".to_string()),
///             version: "1.0.0".to_string(),
///             timeout_seconds: Some(600),
///             tags: vec!["orders".to_string()],
///             namespace: Some("default".to_string()),
///             max_concurrent_steps: None,
///         }),
///     }
/// }
/// ```
pub fn generate_sdk_workflow_registration(
    fn_name: &Ident,
    attrs: &WorkflowAttrs,
    handler_wrapper_name: &Ident,
) -> TokenStream {
    let workflow_name = attrs
        .name
        .as_ref()
        .map(|s| s.to_string())
        .unwrap_or_else(|| fn_name.to_string());

    let _description = attrs
        .description
        .as_ref()
        .map(|d| quote! { Some(#d.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _version = &attrs.version;

    let _timeout_seconds = attrs
        .timeout
        .map(|t| quote! { Some(#t) })
        .unwrap_or_else(|| quote! { None });

    let _namespace = attrs
        .namespace
        .as_ref()
        .map(|ns| quote! { Some(#ns.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _task_queue = attrs
        .task_queue
        .as_ref()
        .map(|tq| quote! { Some(#tq.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let max_concurrent = attrs.max_concurrent;
    let _max_concurrent_code = if max_concurrent > 0 {
        quote! { Some(#max_concurrent as u32) }
    } else {
        quote! { None }
    };

    let _max_concurrent_executions = attrs
        .max_concurrent_executions
        .map(|mce| quote! { Some(#mce) })
        .unwrap_or_else(|| quote! { None });

    // `cron` takes precedence over `schedule` when both are set.
    let _cron_schedule = attrs
        .cron
        .as_ref()
        .or(attrs.schedule.as_ref())
        .map(|cron| quote! { Some(#cron.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _auto_heartbeat = attrs.auto_heartbeat;

    let _heartbeat_interval_seconds = attrs
        .heartbeat_interval
        .map(|hi| quote! { Some(#hi) })
        .unwrap_or_else(|| quote! { None });

    let _on_timeout = attrs
        .on_timeout
        .as_ref()
        .map(|ot| quote! { Some(#ot.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let _retry_policy = if let Some(ref retry_attr) = attrs.retry_policy {
        let max_attempts = retry_attr.max_attempts;
        let initial_interval = retry_attr.initial_interval;
        let max_interval = retry_attr.max_interval;
        let backoff_coefficient = retry_attr.backoff_coefficient;

        // Attribute values are whole seconds, because attribute literals cannot be
        // `Duration`s.
        quote! {
            Some(
                <orcher::task::RetryPolicy as ::core::default::Default>::default()
                    .with_max_attempts(#max_attempts)
                    .with_initial_interval(::std::time::Duration::from_secs(#initial_interval))
                    .with_max_interval(::std::time::Duration::from_secs(#max_interval))
                    .with_backoff_coefficient(#backoff_coefficient),
            )
        }
    } else {
        quote! { None }
    };

    let tags_vec = &attrs.tags;
    let _tags = quote! {
        vec![#(#tags_vec.to_string()),*]
    };

    let register_fn_name = syn::Ident::new(
        &format!("__orcher_register_workflow_{}", fn_name),
        fn_name.span(),
    );

    quote! {
        // Builds the handler at run time and inserts it into the worker registry.
        #[doc(hidden)]
        fn #register_fn_name(registry: &mut orcher::worker::Registry) {
            use orcher::worker::registration::*;

            let handler: orcher::worker::registration::WorkflowHandlerFn = std::sync::Arc::new(|ctx, input| {
                Box::pin(async move {
                    #handler_wrapper_name(ctx, input).await
                }) as orcher::worker::registration::BoxFuture<'static, std::result::Result<orcher::Payload, orcher::Error>>
            });

            let workflow_handler = orcher::worker::registry::WorkflowHandler {
                name: #workflow_name.to_string(),
                handler: Some(handler),
            };

            registry.workflows.insert(#workflow_name.to_string(), workflow_handler);
        }

        // Submit only the function pointer: `inventory::submit!` requires a const value.
        ::orcher::__private::inventory::submit! {
            orcher::worker::registration::WorkflowRegistration::new(#register_fn_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("512M"), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512MB"), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512Mi"), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("512MiB"), 512 * 1024 * 1024);

        assert_eq!(parse_memory_size("1G"), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1GB"), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1Gi"), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1GiB"), 1024 * 1024 * 1024);

        assert_eq!(parse_memory_size("1024K"), 1024 * 1024);
        assert_eq!(parse_memory_size("1024KB"), 1024 * 1024);

        assert_eq!(parse_memory_size("1048576"), 1048576);
    }

    #[test]
    fn test_parse_memory_size_case_insensitive() {
        assert_eq!(parse_memory_size("512mb"), 512 * 1024 * 1024);
        assert_eq!(parse_memory_size("1gb"), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("512Mi"), 512 * 1024 * 1024);
    }
}
