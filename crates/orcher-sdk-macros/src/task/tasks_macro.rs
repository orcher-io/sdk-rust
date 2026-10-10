//! Implementation of the `#[tasks]` attribute macro.
//!
//! `#[tasks]` goes on an inherent impl block and turns each `#[task]` method into a task.
//! Group-level settings apply to every task in the block; a method's own `#[task(...)]`
//! arguments override them.
//!
//! The macro is documented, with examples, on `#[tasks]` in the crate root.

use crate::common::attrs::{
    expand_task_shortcuts, ineffective_attr_warnings, parse_task_attrs, parse_tasks_group_attrs,
    validate_approval, validate_condition, validate_resources, validate_retry_policy,
    validate_task_attrs, TaskAttrs, TasksGroupAttrs,
};
use crate::common::codegen::{extract_method_fn_info, generate_sdk_task_wrapper};
use crate::common::integration::{
    extract_sdk_input_type, extract_sdk_output_type, uses_sdk_task_context,
};
use crate::common::registration::generate_sdk_task_registration;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ImplItem, ImplItemFn, ItemImpl, Meta};

/// Entry point for `#[tasks]`: parses the group arguments and expands the impl block.
pub fn tasks_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let impl_block = parse_macro_input!(item as ItemImpl);

    let group_attrs = if attr.is_empty() {
        TasksGroupAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();
        let wrapped_tokens = quote::quote! { tasks(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match parse_tasks_group_attrs(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse tasks group attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match tasks_impl_inner(group_attrs, impl_block) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn tasks_impl_inner(
    group_attrs: TasksGroupAttrs,
    impl_block: ItemImpl,
) -> Result<TokenStream2, syn::Error> {
    if impl_block.trait_.is_some() {
        return Err(syn::Error::new_spanned(
            &impl_block,
            "Task groups cannot be defined on trait implementations",
        ));
    }

    if !impl_block.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &impl_block.generics,
            "Task groups do not support generic impl blocks",
        ));
    }

    let struct_name = if let syn::Type::Path(type_path) = &*impl_block.self_ty {
        if let Some(segment) = type_path.path.segments.last() {
            segment.ident.clone()
        } else {
            return Err(syn::Error::new_spanned(
                &impl_block.self_ty,
                "Expected a named struct type",
            ));
        }
    } else {
        return Err(syn::Error::new_spanned(
            &impl_block.self_ty,
            "Expected a named struct type",
        ));
    };

    // Generated free functions are prefixed with the snake_case struct name so that task
    // methods with the same name on different structs do not collide.
    let prefix = pascal_to_snake(&struct_name.to_string());

    let mut non_task_items: Vec<&ImplItem> = Vec::new();
    let mut task_methods: Vec<&ImplItemFn> = Vec::new();

    for item in &impl_block.items {
        if let ImplItem::Fn(method) = item {
            if has_task_attr(method) {
                task_methods.push(method);
            } else {
                non_task_items.push(item);
            }
        } else {
            non_task_items.push(item);
        }
    }

    let mut free_functions = Vec::new();
    let mut associated_consts = Vec::new();

    for method in &task_methods {
        let generated = process_task_method(method, &group_attrs, &prefix)?;
        free_functions.push(generated.free_functions);
        associated_consts.push(generated.associated_const);
    }

    let group_warnings = ineffective_attr_warnings("tasks", &group_attrs.ineffective);

    let expanded = quote! {
        #group_warnings

        // Task code is emitted as module-level items; see `TaskMethodOutput`.
        #(#free_functions)*

        // Task constants plus every non-task item, left as written.
        impl #struct_name {
            #(#associated_consts)*

            #(#non_task_items)*
        }
    };

    Ok(expanded)
}

/// Code generated for one `#[task]` method.
struct TaskMethodOutput {
    /// Module-level items: the `_impl` function, the handler, the task reference type and the
    /// registration.
    free_functions: TokenStream2,
    /// Associated constant on the struct, such as `pub const charge: ...`.
    associated_const: TokenStream2,
}

/// Expands one `#[task]` method: merges group defaults, validates, and generates its items.
fn process_task_method(
    method: &ImplItemFn,
    group_attrs: &TasksGroupAttrs,
    prefix: &str,
) -> Result<TaskMethodOutput, syn::Error> {
    let mut attrs = parse_task_attrs_from_method(method)?;

    group_attrs.merge_into(&mut attrs);

    // Same shortcut expansion as a standalone `#[task]`.
    expand_task_shortcuts(&mut attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid task configuration: {}", e),
        )
    })?;

    validate_merged_attrs(&attrs)?;

    let method_name = &method.sig.ident;
    let method_vis = &method.vis;
    let method_body = &method.block;
    let method_attrs: Vec<_> = method
        .attrs
        .iter()
        .filter(|a| !a.path().is_ident("task"))
        .collect();

    let fn_info = extract_method_fn_info(method);

    if !fn_info.is_async {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Task methods must be async",
        ));
    }

    if let Some(syn::FnArg::Receiver(_)) = fn_info.inputs.first() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Task methods must be associated functions, not instance methods. \
             Use `ctx: TaskContext` as the first parameter instead of `&self`.",
        ));
    }

    if !uses_sdk_task_context(&fn_info) {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Task methods must use TaskContext as the first parameter.",
        ));
    }

    if let syn::ReturnType::Default = fn_info.output {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Task methods must have a return type",
        ));
    }

    let fn_name_impl = format_ident!("{}_{}_impl", prefix, method_name);
    let handler_wrapper_name = format_ident!("{}_{}_handler", prefix, method_name);
    let task_ref_name = format_ident!("{}_{}_task_ref", prefix, method_name);

    // The task is registered under the method name (or `name`), not the prefixed name.
    let task_name_str = attrs
        .name
        .as_ref()
        .map(|s| s.to_string())
        .unwrap_or_else(|| method_name.to_string());

    let fn_inputs = &fn_info.inputs;
    let fn_output = &fn_info.output;

    let input_type = extract_sdk_input_type(&fn_info);
    let output_type = extract_sdk_output_type(fn_output);

    let wrapper_fn = generate_sdk_task_wrapper(
        &fn_info,
        &fn_name_impl,
        &handler_wrapper_name,
        input_type,
        output_type,
    );

    // Registration derives the task name from `attrs.name`, so set it to the unprefixed name.
    let mut reg_attrs = attrs.clone();
    reg_attrs.name = Some(task_name_str.clone());
    let registration = generate_sdk_task_registration(
        &format_ident!("{}_{}", prefix, method_name),
        &reg_attrs,
        &handler_wrapper_name,
    );

    let ineffective_warnings = ineffective_attr_warnings("task", &attrs.ineffective);

    let free_functions = quote! {
        #ineffective_warnings

        #(#method_attrs)*
        #method_vis async fn #fn_name_impl(#fn_inputs) #fn_output {
            #method_body
        }

        #wrapper_fn

        #[automatically_derived]
        #[derive(Debug, Clone, Copy)]
        #[allow(non_camel_case_types)]
        pub struct #task_ref_name;

        impl ::orcher_sdk::task::TaskReference for #task_ref_name {
            fn task_name(&self) -> &'static str {
                #task_name_str
            }
        }

        #registration
    };

    let associated_const = quote! {
        #[allow(non_upper_case_globals)]
        pub const #method_name: #task_ref_name = #task_ref_name;
    };

    Ok(TaskMethodOutput {
        free_functions,
        associated_const,
    })
}

/// Returns whether a method carries a `#[task]` attribute.
fn has_task_attr(method: &ImplItemFn) -> bool {
    method.attrs.iter().any(|attr| attr.path().is_ident("task"))
}

/// Parses the method's `#[task]` or `#[task(...)]` arguments.
fn parse_task_attrs_from_method(method: &ImplItemFn) -> Result<TaskAttrs, syn::Error> {
    for attr in &method.attrs {
        if attr.path().is_ident("task") {
            if let Meta::Path(_) = &attr.meta {
                return Ok(TaskAttrs::default());
            }

            return parse_task_attrs(&attr.meta).map_err(|e| {
                syn::Error::new_spanned(attr, format!("Failed to parse task attributes: {}", e))
            });
        }
    }
    Ok(TaskAttrs::default())
}

/// Validates the task attributes after group defaults and shortcuts have been applied.
fn validate_merged_attrs(attrs: &TaskAttrs) -> Result<(), syn::Error> {
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

    validate_task_attrs(attrs).map_err(|e| {
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

    Ok(())
}

/// Converts a PascalCase name to snake_case.
fn pascal_to_snake(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_pascal_to_snake() {
        assert_eq!(pascal_to_snake("PaymentTasks"), "payment_tasks");
        assert_eq!(pascal_to_snake("Counter"), "counter");
        assert_eq!(pascal_to_snake("HTTPClient"), "h_t_t_p_client");
        assert_eq!(pascal_to_snake("MyAPI"), "my_a_p_i");
        assert_eq!(pascal_to_snake("simple"), "simple");
    }

    #[test]
    fn test_has_task_attr() {
        let method: ImplItemFn = parse_quote! {
            #[task]
            async fn charge(ctx: TaskContext, p: Payment) -> Result<Confirmation> {
                Ok(Confirmation {})
            }
        };
        assert!(has_task_attr(&method));

        let method: ImplItemFn = parse_quote! {
            fn helper() -> String {
                "hello".to_string()
            }
        };
        assert!(!has_task_attr(&method));
    }

    #[test]
    fn test_parse_task_attrs_empty() {
        let method: ImplItemFn = parse_quote! {
            #[task]
            async fn charge(ctx: TaskContext, p: Payment) -> Result<Confirmation> {
                Ok(Confirmation {})
            }
        };
        let attrs = parse_task_attrs_from_method(&method).unwrap();
        assert!(attrs.retry.is_none());
        assert!(attrs.timeout.is_none());
    }

    #[test]
    fn test_parse_task_attrs_with_args() {
        let method: ImplItemFn = parse_quote! {
            #[task(retry = 10, timeout = 120)]
            async fn refund(ctx: TaskContext, r: Refund) -> Result<RefundResult> {
                Ok(RefundResult {})
            }
        };
        let attrs = parse_task_attrs_from_method(&method).unwrap();
        assert_eq!(attrs.retry, Some(10));
        assert_eq!(attrs.timeout, Some(120));
    }

    #[test]
    fn test_tasks_impl_inner_basic() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tasks_impl_inner_rejects_trait_impl() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl SomeTrait for MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }

    #[test]
    fn test_tasks_impl_inner_rejects_generic() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl<T> MyTasks<T> {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }

    #[test]
    fn test_tasks_impl_inner_rejects_self_receiver() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(&self, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }

    #[test]
    fn test_tasks_impl_inner_preserves_non_task_methods() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }

                fn helper() -> String {
                    "hello".to_string()
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_ok());
        let tokens = result.unwrap().to_string();
        assert!(tokens.contains("helper"));
    }

    #[test]
    fn test_tasks_impl_inner_with_group_attrs() {
        let group_attrs = TasksGroupAttrs {
            retry: Some(3),
            timeout: Some(60),
            ..Default::default()
        };
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_ok());
    }

    #[test]
    fn test_tasks_impl_generates_associated_const() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_ok());
        let tokens = result.unwrap().to_string();
        // The associated constant keeps the method name.
        assert!(tokens.contains("do_thing"));
        // The reference type is prefixed with the struct name.
        assert!(tokens.contains("my_tasks_do_thing_task_ref"));
    }

    #[test]
    fn test_tasks_impl_rejects_non_async() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                fn do_thing(ctx: TaskContext, input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }

    #[test]
    fn test_tasks_impl_rejects_no_return_type() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(ctx: TaskContext, input: String) {
                    // no return type
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }

    #[test]
    fn test_tasks_impl_rejects_no_task_context() {
        let group_attrs = TasksGroupAttrs::default();
        let impl_block: ItemImpl = parse_quote! {
            impl MyTasks {
                #[task]
                async fn do_thing(input: String) -> Result<String> {
                    Ok(input)
                }
            }
        };

        let result = tasks_impl_inner(group_attrs, impl_block);
        assert!(result.is_err());
    }
}
