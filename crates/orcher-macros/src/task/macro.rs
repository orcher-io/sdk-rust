//! Implementation of the `#[task]` attribute macro.
//!
//! The macro expands shortcuts and presets into full task configuration, validates it, and
//! generates the payload-level handler, a type-safe task reference, and the `inventory`
//! registration that makes the task available to workers.

use crate::common::attrs::{
    validate_approval, validate_condition, validate_resources, validate_retry_policy,
    validate_task_attrs,
};
use crate::common::codegen::{
    extract_fn_info, generate_sdk_task_wrapper, task_wrapper_name, uses_task_context,
};
use crate::common::context::{generate_legacy_wrapper, generate_task_context_wrapper};
use crate::common::integration::{
    extract_sdk_input_type, extract_sdk_output_type, uses_sdk_task_context,
};
use crate::common::registration::generate_sdk_task_registration;
use darling::FromMeta;
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
            Ok(meta) => match crate::common::attrs::TaskAttrs::from_meta(&meta) {
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

    // A preset only fills in values the user did not set explicitly.
    if let Some(preset_name) = &attrs.preset {
        let preset = get_preset(preset_name);
        if attrs.retry.is_none() && preset.retry.is_some() {
            attrs.retry = preset.retry;
        }
        if attrs.timeout_mins.is_none() && preset.timeout_mins.is_some() {
            attrs.timeout_mins = preset.timeout_mins;
        }
        if attrs.memory.is_none() && preset.memory.is_some() {
            attrs.memory = preset.memory;
        }
        if attrs.timeout.is_none() && preset.timeout.is_some() {
            attrs.timeout = preset.timeout;
        }
    }

    // `retry = N` expands to an exponential policy (1s initial, 60s max, factor 2.0) unless
    // an explicit `retry_policy` is given.
    if let Some(max_attempts) = attrs.retry {
        if attrs.retry_policy.is_none() {
            attrs.retry_policy = Some(crate::common::attrs::RetryPolicyAttr {
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

    // `memory` fills in the memory request without overriding an explicit one.
    if let Some(memory) = &attrs.memory {
        if attrs.resources.is_none() {
            attrs.resources = Some(crate::common::attrs::ResourcesAttr {
                cpu: None,
                memory: Some(memory.clone()),
                disk: None,
                cpu_limit: None,
                memory_limit: None,
                disk_limit: None,
                network_limit: None,
            });
        } else if let Some(ref mut resources) = attrs.resources {
            if resources.memory.is_none() {
                resources.memory = Some(memory.clone());
            }
        }
    }

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
            "Task functions must use orcher::TaskContext as the first parameter. \
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

            impl ::orcher::task::TaskReference for #task_ref_name {
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

    let expanded = quote! {
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

/// Returns the attribute defaults for a named preset (`long-running`, `quick`, `critical`).
fn get_preset(name: &str) -> crate::common::attrs::TaskAttrs {
    match name {
        "long-running" => crate::common::attrs::TaskAttrs {
            retry: Some(3),
            timeout_mins: Some(30),
            memory: Some("512Mi".to_string()),
            ..Default::default()
        },
        "quick" => crate::common::attrs::TaskAttrs {
            timeout_mins: Some(1),
            retry: Some(0),
            memory: Some("128Mi".to_string()),
            ..Default::default()
        },
        "critical" => crate::common::attrs::TaskAttrs {
            retry: Some(5),
            timeout_mins: Some(60),
            memory: Some("1Gi".to_string()),
            ..Default::default()
        },
        _ => {
            // An unknown preset contributes nothing.
            crate::common::attrs::TaskAttrs::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use darling::FromMeta;
    use syn::parse_quote;

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
    fn test_task_impl_with_namespace() {
        let meta: Meta = parse_quote!(task(name = "test_task", namespace = "production"));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.namespace, Some("production".to_string()));

        let item: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = task_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_impl_with_version_and_namespace() {
        let meta: Meta = parse_quote!(task(
            name = "payment_task",
            version = "3.1.0",
            namespace = "tenant-xyz"
        ));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.version, Some("3.1.0".to_string()));
        assert_eq!(attrs.namespace, Some("tenant-xyz".to_string()));

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

        // Version and namespace stay unset unless given explicitly.
        assert_eq!(attrs.version, None);

        assert_eq!(attrs.namespace, None);

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
    fn test_task_impl_multi_tenant() {
        let meta: Meta = parse_quote!(task(
            name = "data_processing",
            version = "2.5.1",
            namespace = "customer-acme",
            description = "Process customer data",
            timeout = 300
        ));
        let attrs = crate::common::attrs::TaskAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.name, Some("data_processing".to_string()));
        assert_eq!(attrs.version, Some("2.5.1".to_string()));
        assert_eq!(attrs.namespace, Some("customer-acme".to_string()));
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
