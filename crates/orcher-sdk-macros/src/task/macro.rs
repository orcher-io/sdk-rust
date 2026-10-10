//! Implementation of the `#[task]` attribute macro.
//!
//! The macro expands shortcuts and presets into full task configuration, validates it, and
//! generates the payload-level handler, a type-safe task reference, and the `inventory`
//! registration that makes the task available to workers.

use crate::common::attrs::{
    expand_task_shortcuts, ineffective_attr_warnings, parse_task_attrs, validate_approval,
    validate_condition, validate_resources, validate_retry_policy, validate_task_attrs,
};
use crate::common::codegen::{
    extract_fn_info, generate_sdk_task_wrapper, task_wrapper_name, uses_task_context,
};
use crate::common::context::{generate_legacy_wrapper, generate_task_context_wrapper};
use crate::common::integration::{
    extract_sdk_input_type, extract_sdk_output_type, uses_sdk_task_context,
};
use crate::common::registration::generate_sdk_task_registration;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use syn::{parse_macro_input, ItemFn, Meta};

/// Entry point for `#[task]`: parses the attribute arguments and expands the task.
pub fn task_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as ItemFn);

    let attrs = if attr.is_empty() {
        crate::common::attrs::TaskAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();

        // Wrap the arguments as `task(...)` so darling can parse them as a list.
        let wrapped_tokens = quote::quote! { task(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match parse_task_attrs(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse task attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match task_impl_inner(attrs, item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn task_impl_inner(
    mut attrs: crate::common::attrs::TaskAttrs,
    item_fn: ItemFn,
) -> Result<TokenStream2, syn::Error> {
    let fn_info = extract_fn_info(&item_fn);

    expand_task_shortcuts(&mut attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid task configuration: {}", e),
        )
    })?;

    if let Some(ref retry_policy) = attrs.retry_policy {
        validate_retry_policy(retry_policy).map_err(|e| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid retry policy: {}", e),
            )
        })?;
    }

    if let Some(ref resources) = attrs.resources {
        validate_resources(resources).map_err(|e| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid resource requirements: {}", e),
            )
        })?;
    }

    validate_task_attrs(&attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid task configuration: {}", e),
        )
    })?;

    if let Some(ref condition) = attrs.condition {
        validate_condition(condition).map_err(|e| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid condition expression: {}", e),
            )
        })?;
    }

    validate_approval(attrs.requires_approval, attrs.approval_timeout).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid approval configuration: {}", e),
        )
    })?;

    let fn_name = &fn_info.name;
    let fn_inputs = &fn_info.inputs;
    let fn_output = &fn_info.output;
    let fn_body = &item_fn.block;
    let fn_vis = &item_fn.vis;
    let fn_attrs = &item_fn.attrs;

    if !fn_info.is_async {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "Task functions must be async",
        ));
    }

    let _input_names: Vec<_> = fn_inputs
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat_type) = arg {
                if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                    return Some(&pat_ident.ident);
                }
            }
            None
        })
        .collect();

    let wrapper_name = task_wrapper_name(fn_name);

    let _output_type = match fn_output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &item_fn.sig,
                "Task functions must have a return type",
            ));
        }
    };

    // Both checks match any first parameter whose type is named `TaskContext`; the SDK
    // branches below take precedence.
    let has_task_context = uses_task_context(&fn_info);

    let has_sdk_context = uses_sdk_task_context(&fn_info);

    let handler_wrapper_name = syn::Ident::new(&format!("{}_handler", fn_name), fn_name.span());

    // Registration requires the SDK `TaskContext` as the first parameter, so the non-SDK
    // branches below are unreachable.
    let registration = if has_sdk_context {
        generate_sdk_task_registration(fn_name, &attrs, &handler_wrapper_name)
    } else {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "Task functions must use orcher_sdk::TaskContext as the first parameter. \
             Legacy (non-SDK) task registration is no longer supported.",
        ));
    };

    let condition_code = attrs
        .condition
        .as_ref()
        .map(|c| {
            quote! { Some(#c.to_string()) }
        })
        .unwrap_or_else(|| quote! { None });

    let parallel_flag = attrs.parallel;
    let requires_approval_flag = attrs.requires_approval;

    let approval_timeout_code = attrs
        .approval_timeout
        .map(|t| {
            quote! { Some(#t) }
        })
        .unwrap_or_else(|| quote! { None });

    let approvers_code = attrs
        .approvers
        .as_ref()
        .map(|a| {
            quote! { Some(#a.to_string()) }
        })
        .unwrap_or_else(|| quote! { None });

    // The user's function is renamed to `<name>_impl` so that `<name>` can be the task
    // reference constant.
    let fn_name_impl = syn::Ident::new(&format!("{}_impl", fn_name), fn_name.span());

    let wrapper_fn = if has_sdk_context {
        let input_type = extract_sdk_input_type(&fn_info);
        let output_type = extract_sdk_output_type(fn_output);
        generate_sdk_task_wrapper(
            &fn_info,
            &fn_name_impl,
            &handler_wrapper_name,
            input_type,
            output_type,
        )
    } else if has_task_context {
        generate_task_context_wrapper(&fn_info, &item_fn, &handler_wrapper_name, &fn_name_impl)
    } else {
        generate_legacy_wrapper(&fn_info, &item_fn, &handler_wrapper_name, &fn_name_impl)
    };

    // The task reference lets callers write `ctx.execute_task(send_email, input)` with a
    // name the compiler checks.
    let task_reference_struct = {
        let task_name_str = fn_name.to_string();
        let task_ref_name = syn::Ident::new(&format!("{}_task_ref", fn_name), fn_name.span());
        quote! {
            /// Type-safe reference to a task.
            ///
            /// Generated by `#[task]` so that callers name the task through a checked item
            /// instead of a string, which catches typos and renames at compile time. The
            /// constant with the task function's name is passed to
            /// `WorkflowContext::execute_task`.
            #[automatically_derived]
            #[derive(Debug, Clone, Copy)]
            #[allow(non_camel_case_types)]
            pub struct #task_ref_name;

            impl ::orcher_sdk::task::TaskReference for #task_ref_name {
                fn task_name(&self) -> &'static str {
                    #task_name_str
                }
            }

            /// Reference constant with the same name as the task function.
            ///
            /// The task function itself is renamed to `<name>_impl`, so this name is free.
            #[allow(non_upper_case_globals)]
            pub const #fn_name: #task_ref_name = #task_ref_name;
        }
    };

    let wrapper_impl = if has_sdk_context {
        // Registration carries all task metadata, so no wrapper struct is generated.
        quote! {}
    } else {
        quote! {
            #[automatically_derived]
            #[doc = concat!("Task wrapper for `", stringify!(#fn_name), "`")]
            #[allow(non_camel_case_types)]
            pub struct #wrapper_name;

            #[automatically_derived]
            impl #wrapper_name {
                /// Returns the task name.
                pub fn name() -> &'static str {
                    stringify!(#fn_name)
                }

                /// Always returns `None`; the retry policy is carried by the registration.
                pub fn retry_policy() -> Option<()> {
                    None
                }

                /// Returns the condition expression that gates execution, if any.
                pub fn condition() -> Option<String> {
                    #condition_code
                }

                /// Returns whether the task may run in parallel.
                pub fn is_parallel() -> bool {
                    #parallel_flag
                }

                /// Returns whether the task requires human approval.
                pub fn requires_approval() -> bool {
                    #requires_approval_flag
                }

                /// Returns the approval timeout in seconds, if configured.
                pub fn approval_timeout() -> Option<u64> {
                    #approval_timeout_code
                }

                /// Returns the approver roles, if configured.
                pub fn approvers() -> Option<String> {
                    #approvers_code
                }
            }
        }
    };

    let ineffective_warnings = ineffective_attr_warnings("task", &attrs.ineffective);

    let expanded = quote! {
        #ineffective_warnings

        #(#fn_attrs)*
        #fn_vis async fn #fn_name_impl(#fn_inputs) #fn_output {
            #fn_body
        }

        #wrapper_fn

        #wrapper_impl

        #task_reference_struct

        #registration
    };

    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use darling::FromMeta;
    use syn::parse_quote;

    fn expand(meta: Meta) -> Result<String, syn::Error> {
        let attrs = parse_task_attrs(&meta)
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), e.to_string()))?;
        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: String) -> Result<String> {
                Ok(input)
            }
        };
        task_impl_inner(attrs, item).map(|tokens| tokens.to_string())
    }

    #[test]
    fn each_ineffective_attribute_is_reported_as_deprecated() {
        for (meta, key, replacement) in [
            (
                parse_quote!(task(task_queue = "q")),
                "task_queue",
                "WorkerBuilder::task_queue",
            ),
            (parse_quote!(task(priority = 5)), "priority", "no priority"),
            (
                parse_quote!(task(max_concurrent = 2)),
                "max_concurrent",
                "max_concurrent_tasks",
            ),
            (
                parse_quote!(task(rate_limit = 2)),
                "rate_limit",
                "not rate limited",
            ),
            (
                parse_quote!(task(memory = "1Gi")),
                "memory",
                "size the worker",
            ),
            (
                parse_quote!(task(resources(cpu = 1.0))),
                "resources",
                "size the worker",
            ),
            (parse_quote!(task(version = "2")), "version", "version_id"),
            (
                parse_quote!(task(description = "d")),
                "description",
                "doc comment",
            ),
            (
                parse_quote!(task(idempotency_key = "k")),
                "idempotency_key",
                "with_task_id",
            ),
            (
                parse_quote!(task(parallel)),
                "parallel",
                "awaiting them together",
            ),
        ] {
            let expanded = expand(meta).unwrap();
            let name = format!("{key}_has_no_effect_on_task");
            assert!(
                expanded.contains("deprecated") && expanded.contains(&name),
                "`{key}` produced no deprecation warning: {expanded}"
            );
            assert!(
                expanded.contains(replacement),
                "the `{key}` warning does not say `{replacement}`"
            );
        }
    }

    #[test]
    fn effective_attributes_produce_no_warning() {
        let expanded = expand(parse_quote!(task(
            name = "t",
            timeout = 30,
            heartbeat_timeout = 5,
            retry = 3,
            non_retryable_errors = ["Declined"],
            preset = "critical"
        )))
        .unwrap();
        assert!(!expanded.contains("deprecated"), "{expanded}");
    }

    #[test]
    fn namespace_is_rejected_with_where_to_set_it() {
        let meta: Meta = parse_quote!(task(name = "t", namespace = "production"));
        let message = parse_task_attrs(&meta)
            .expect_err("`namespace` must not be accepted on #[task]")
            .to_string();
        assert!(message.contains("not a #[task] option"), "{message}");
        assert!(message.contains("WorkerBuilder::namespace"), "{message}");
    }

    #[test]
    fn the_quick_preset_makes_one_attempt() {
        let meta: Meta = parse_quote!(task(preset = "quick"));
        let mut attrs = parse_task_attrs(&meta).unwrap();
        expand_task_shortcuts(&mut attrs).unwrap();
        assert_eq!(attrs.retry_policy.unwrap().max_attempts, 1);
        assert_eq!(attrs.timeout, Some(60));

        assert!(expand(parse_quote!(task(preset = "quick"))).is_ok());
    }

    #[test]
    fn an_unknown_preset_is_rejected() {
        let err = expand(parse_quote!(task(preset = "speedy"))).unwrap_err();
        assert!(err.to_string().contains("unknown preset `speedy`"), "{err}");
    }

    #[test]
    fn test_task_impl_basic() {
        let attrs = crate::common::attrs::TaskAttrs::default();
        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_version() {
        let meta: Meta = parse_quote!(task(name = "test_task", version = "2.0.0"));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.version, Some("2.0.0".to_string()));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_defaults() {
        let attrs = crate::common::attrs::TaskAttrs::default();

        // Version stays unset unless given explicitly.
        assert_eq!(attrs.version, None);

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_retry_shortcut() {
        let meta: Meta = parse_quote!(task(retry = 5));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.retry, Some(5));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_timeout_mins_shortcut() {
        let meta: Meta = parse_quote!(task(timeout_mins = 5));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.timeout_mins, Some(5));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_memory_shortcut() {
        let meta: Meta = parse_quote!(task(memory = "512Mi"));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.memory, Some("512Mi".to_string()));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_metadata() {
        let meta: Meta = parse_quote!(task(
            name = "data_processing",
            version = "2.5.1",
            description = "Process customer data",
            timeout = 300
        ));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.name, Some("data_processing".to_string()));
        assert_eq!(attrs.version, Some("2.5.1".to_string()));
        assert_eq!(attrs.description, Some("Process customer data".to_string()));
        assert_eq!(attrs.timeout, Some(300));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_without_sdk_context_fails() {
        let attrs = crate::common::attrs::TaskAttrs::default();
        let item: ItemFn = parse_quote! {
            async fn my_task() -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_impl_non_async_fails() {
        let attrs = crate::common::attrs::TaskAttrs::default();
        let item: ItemFn = parse_quote! {
            fn my_task() -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_err());
    }
}
