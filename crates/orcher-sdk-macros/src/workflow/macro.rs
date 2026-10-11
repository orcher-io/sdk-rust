//! Implementation of the `#[workflow]` attribute macro.
//!
//! The macro keeps the original function and generates a payload-level handler, a
//! `<Name>` metadata struct with an `execute` method, and the `inventory` registration that
//! makes the workflow available to workers.

use crate::common::attrs::{
    validate_retry_policy, validate_schedule, validate_workflow_attrs, IneffectiveWorkflowAttr,
};
use crate::common::codegen::{
    extract_fn_info, generate_sdk_workflow_wrapper, workflow_wrapper_name,
};
use crate::common::integration::{
    extract_sdk_input_type, extract_sdk_output_type, uses_sdk_workflow_context,
};
use crate::common::registration::generate_sdk_workflow_registration;
use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Meta};

/// Entry point for `#[workflow]`: parses the attribute arguments and expands the workflow.
pub fn workflow_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as ItemFn);

    let attrs = if attr.is_empty() {
        crate::common::attrs::WorkflowAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();

        // Wrap the arguments as `workflow(...)` so darling can parse them as a list.
        let wrapped_tokens = quote::quote! { workflow(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match crate::common::attrs::WorkflowAttrs::from_meta(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse workflow attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match workflow_impl_inner(attrs, item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn workflow_impl_inner(
    attrs: crate::common::attrs::WorkflowAttrs,
    item_fn: ItemFn,
) -> Result<TokenStream2, syn::Error> {
    let fn_info = extract_fn_info(&item_fn);

    validate_workflow_return_type(&item_fn)?;
    if let Some(ref retry_policy) = attrs.retry_policy {
        validate_retry_policy(retry_policy).map_err(|e| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid retry policy: {}", e),
            )
        })?;
    }

    if let Some(ref schedule) = attrs.schedule {
        validate_schedule(schedule).map_err(|e| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("Invalid schedule expression: {}", e),
            )
        })?;
    }

    validate_workflow_attrs(&attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid workflow configuration: {}", e),
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
            "Workflow functions must be async",
        ));
    }

    // Argument names, used by `execute` to forward the call.
    let input_names: Vec<_> = fn_inputs
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

    let wrapper_name = workflow_wrapper_name(fn_name);

    let has_sdk_context = uses_sdk_workflow_context(&fn_info);

    let _output_type = match fn_output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &item_fn.sig,
                "Workflow functions must have a return type",
            ));
        }
    };

    let handler_wrapper_name = syn::Ident::new(&format!("{}_handler", fn_name), fn_name.span());

    let wrapper_fn = if has_sdk_context {
        let input_type = extract_sdk_input_type(&fn_info);
        let output_type_extracted = extract_sdk_output_type(fn_output);
        generate_sdk_workflow_wrapper(
            &fn_info,
            fn_name,
            &handler_wrapper_name,
            input_type,
            output_type_extracted,
        )
    } else {
        quote! {}
    };

    // Registration requires `WorkflowContext` as the first parameter.
    let registration = if has_sdk_context {
        generate_sdk_workflow_registration(fn_name, &attrs, &handler_wrapper_name)
    } else {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "Workflow functions must use orcher_sdk::WorkflowContext as the first parameter. \
             Legacy (non-SDK) workflow registration is no longer supported.",
        ));
    };

    // `cron` takes precedence over `schedule` when both are set.
    let schedule_code = attrs
        .cron
        .as_ref()
        .or(attrs.schedule.as_ref())
        .map(|s| {
            quote! { Some(#s.to_string()) }
        })
        .unwrap_or_else(|| quote! { None });

    let enabled_flag = attrs.enabled;

    // The name the worker registers the workflow under: the declared `name`, or the
    // function name when none is given.
    let workflow_name = attrs.name.clone().unwrap_or_else(|| fn_name.to_string());

    let ineffective_warnings = ineffective_attr_warnings(&attrs);

    let expanded = quote! {
        #ineffective_warnings

        #(#fn_attrs)*
        #fn_vis async fn #fn_name(#fn_inputs) #fn_output {
            #fn_body
        }

        #wrapper_fn

        #[automatically_derived]
        #[doc = concat!("Workflow wrapper for `", stringify!(#fn_name), "`")]
        #[allow(non_camel_case_types)]
        pub struct #wrapper_name;

        #[automatically_derived]
        impl #wrapper_name {
            /// Returns the name the workflow is registered and started under.
            pub fn name() -> &'static str {
                #workflow_name
            }

            /// Returns the cron schedule declared on the workflow, if any.
            pub fn schedule() -> Option<String> {
                #schedule_code
            }

            /// Returns whether the workflow is enabled.
            pub fn is_enabled() -> bool {
                #enabled_flag
            }

            /// Runs the workflow function directly.
            pub async fn execute(#fn_inputs) #fn_output {
                #fn_name(#(#input_names),*).await
            }
        }

        #registration
    };

    Ok(expanded)
}

/// Explains, per key, why an accepted `#[workflow]` attribute has no effect and what to
/// use instead.
fn ineffective_attr_note(attr: IneffectiveWorkflowAttr) -> &'static str {
    match attr {
        IneffectiveWorkflowAttr::Timeout => {
            "`timeout` on #[workflow] has no effect and will be removed; set the \
             workflow's timeout when you start it, with \
             StartWorkflowOptions::with_workflow_execution_timeout"
        }
        IneffectiveWorkflowAttr::Version => {
            "`version` on #[workflow] has no effect and will be removed; to tie \
             executions to a code release, set the worker's version with \
             WorkerBuilder::version_id or ORCHER_VERSION_ID"
        }
    }
}

/// Emits a deprecation warning, at the attribute, for each attribute that is accepted but
/// has no effect.
///
/// A procedural macro cannot emit a warning directly on stable Rust, so this defines a
/// `#[deprecated]` constant carrying the note and uses it with the attribute's span; the
/// compiler then reports the use as a `deprecated` warning. A warning rather than an
/// error keeps code that sets these attributes compiling.
fn ineffective_attr_warnings(attrs: &crate::common::attrs::WorkflowAttrs) -> TokenStream2 {
    let warnings = attrs.ineffective.iter().map(|(attr, span)| {
        let note = ineffective_attr_note(*attr);
        let name = syn::Ident::new(&format!("{}_has_no_effect_on_workflow", attr.key()), *span);
        quote::quote_spanned! {*span=>
            const _: () = {
                #[deprecated(note = #note)]
                #[allow(non_upper_case_globals)]
                const #name: () = ();
                #name
            };
        }
    });
    quote! { #(#warnings)* }
}

/// Checks that the workflow function declares a return type.
///
/// It does not reject `anyhow::Result`, although using it instead of `orcher_sdk::Result` loses
/// the type information that suspension relies on. The prelude exports `orcher_sdk::Result`,
/// which shadows `anyhow::Result` when both are imported.
fn validate_workflow_return_type(item_fn: &ItemFn) -> Result<(), syn::Error> {
    let return_type = match &item_fn.sig.output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &item_fn.sig,
                "Workflow functions must have a return type",
            ));
        }
    };

    if let syn::Type::Path(type_path) = return_type {
        if let Some(last_segment) = type_path.path.segments.last() {
            if last_segment.ident == "Result" {
                // Any `Result` is accepted; see the function docs.
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use darling::FromMeta;
    use syn::parse_quote;

    fn expand(meta: Meta) -> String {
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext, input: String) -> Result<String> {
                Ok(input)
            }
        };
        workflow_impl_inner(attrs, item).unwrap().to_string()
    }

    #[test]
    fn each_ineffective_attribute_is_reported_as_deprecated() {
        for (meta, key, replacement) in [
            (
                parse_quote!(workflow(name = "w", timeout = 600)),
                "timeout",
                "with_workflow_execution_timeout",
            ),
            (
                parse_quote!(workflow(name = "w", version = "2.0.0")),
                "version",
                "version_id",
            ),
        ] {
            let expanded = expand(meta);
            let name = format!("{key}_has_no_effect_on_workflow");
            assert!(
                expanded.contains("deprecated") && expanded.contains(&name),
                "`{key}` produced no deprecation warning: {expanded}"
            );
            assert!(
                expanded.contains(replacement),
                "the `{key}` warning does not name its replacement"
            );
        }
    }

    #[test]
    fn effective_attributes_produce_no_warning() {
        let expanded = expand(parse_quote!(workflow(
            name = "w",
            description = "d",
            tags("a")
        )));
        assert!(!expanded.contains("deprecated"), "{expanded}");
    }

    #[test]
    fn timeout_does_not_bound_the_workflow_by_wall_clock() {
        // A workflow suspends between activations, so a wall-clock bound in the handler
        // would limit one activation and make replay depend on timing.
        let expanded = expand(parse_quote!(workflow(name = "w", timeout = 600)));
        assert!(!expanded.contains("tokio :: time :: timeout"), "{expanded}");
    }

    #[test]
    fn test_workflow_impl_basic() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow"));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_with_description() {
        let meta: Meta = parse_quote!(workflow(
            name = "test_workflow",
            description = "A test workflow"
        ));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_non_async_fails() {
        let attrs = crate::common::attrs::WorkflowAttrs::default();
        let item: ItemFn = parse_quote! {
            fn my_workflow() -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_err());
    }

    #[test]
    fn test_workflow_return_type_validation() {
        let attrs = crate::common::attrs::WorkflowAttrs::default();

        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<String> {
                Ok("test".to_string())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_with_version() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow", version = "2.0.0"));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_with_timeout() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow", timeout = 600));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_with_tags() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow", tags("etl", "production")));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn namespace_is_rejected_with_where_to_set_it() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow", namespace = "production"));
        let err = crate::common::attrs::WorkflowAttrs::from_meta(&meta)
            .expect_err("`namespace` must not be accepted on #[workflow]");
        let message = err.to_string();
        assert!(message.contains("not a #[workflow] option"), "{message}");
        assert!(message.contains("WorkerBuilder::namespace"), "{message}");
        assert!(
            message.contains("StartWorkflowOptions::with_namespace"),
            "{message}"
        );
    }

    #[test]
    fn test_workflow_impl_with_name_and_version() {
        let meta: Meta = parse_quote!(workflow(name = "order_processing", version = "2.1.0"));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.version, "2.1.0");

        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_defaults() {
        let meta: Meta = parse_quote!(workflow(name = "test_workflow"));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();

        assert_eq!(attrs.version, "1.0.0");

        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_multi_tenant() {
        let meta: Meta = parse_quote!(workflow(
            name = "user_workflow",
            version = "3.0.0",
            description = "Customer-specific workflow"
        ));
        let attrs = crate::common::attrs::WorkflowAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.name, Some("user_workflow".to_string()));
        assert_eq!(attrs.version, "3.0.0");
        assert_eq!(
            attrs.description,
            Some("Customer-specific workflow".to_string())
        );

        let item: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext) -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_workflow_impl_without_sdk_context_fails() {
        let attrs = crate::common::attrs::WorkflowAttrs::default();
        let item: ItemFn = parse_quote! {
            async fn my_workflow() -> Result<(), Box<dyn std::error::Error>> {
                Ok(())
            }
        };

        let result = workflow_impl_inner(attrs, item);
        assert!(result.is_err());
    }
}
