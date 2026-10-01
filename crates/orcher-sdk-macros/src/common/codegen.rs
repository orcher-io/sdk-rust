//! Code generation helpers shared by the task and workflow macros.

use crate::common::attrs::WorkflowAttrs;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type};

/// Returns the metadata struct name for a task: `<Name>Task`.
pub fn task_wrapper_name(fn_name: &Ident) -> Ident {
    let wrapper_name = format!("{}Task", capitalize_first(&fn_name.to_string()));
    Ident::new(&wrapper_name, fn_name.span())
}

/// Returns the metadata struct name for a workflow: `<Name>Workflow`.
pub fn workflow_wrapper_name(fn_name: &Ident) -> Ident {
    let wrapper_name = format!("{}Workflow", capitalize_first(&fn_name.to_string()));
    Ident::new(&wrapper_name, fn_name.span())
}

/// Uppercases the first character of `s`.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Captures the parts of a function signature the macros need.
pub fn extract_fn_info(item_fn: &syn::ItemFn) -> FnInfo {
    FnInfo {
        name: item_fn.sig.ident.clone(),
        is_async: item_fn.sig.asyncness.is_some(),
        inputs: item_fn.sig.inputs.clone(),
        output: item_fn.sig.output.clone(),
    }
}

/// Captures the parts of an impl method's signature the macros need.
pub fn extract_method_fn_info(method: &syn::ImplItemFn) -> FnInfo {
    FnInfo {
        name: method.sig.ident.clone(),
        is_async: method.sig.asyncness.is_some(),
        inputs: method.sig.inputs.clone(),
        output: method.sig.output.clone(),
    }
}

/// Name, asyncness, parameters and return type of a user function.
pub struct FnInfo {
    pub name: Ident,
    pub is_async: bool,
    pub inputs: syn::punctuated::Punctuated<syn::FnArg, syn::token::Comma>,
    pub output: syn::ReturnType,
}

/// Returns whether the first parameter's type is named `TaskContext`.
pub fn uses_task_context(fn_info: &FnInfo) -> bool {
    if let Some(syn::FnArg::Typed(pat_type)) = fn_info.inputs.first() {
        if let syn::Type::Path(type_path) = &*pat_type.ty {
            if let Some(segment) = type_path.path.segments.last() {
                return segment.ident == "TaskContext";
            }
        }
    }
    false
}

/// Returns the type of the first parameter.
#[allow(dead_code)]
pub fn extract_first_param_type(fn_info: &FnInfo) -> Option<&Type> {
    if let Some(syn::FnArg::Typed(pat_type)) = fn_info.inputs.first() {
        return Some(&*pat_type.ty);
    }
    None
}

/// Generates the payload-level handler for a task.
///
/// The handler deserializes the input payload into the task's second parameter type, calls
/// the task function with the `TaskContext`, and serializes the success value. If the
/// signature has no input parameter, a non-`Result` return type, or a context parameter that
/// is not a plain identifier, the handler returns an error at run time instead.
///
/// # Generated Code Pattern
///
/// ```text
/// async fn my_task_handler(
///     ctx: orcher_sdk::TaskContext,
///     input_payload: orcher_sdk::Payload,
/// ) -> Result<orcher_sdk::Payload, orcher_sdk::Error> {
///     use orcher_sdk::prelude::*;
///     use orcher_sdk::payload::{from_payload, to_payload};
///
///     // Deserialize the input.
///     let input: MyInput = from_payload(&input_payload)?;
///
///     // Call the task function.
///     let result = my_task(ctx, input).await?;
///
///     // Serialize the output.
///     let output_payload = to_payload(&result)?;
///     Ok(output_payload)
/// }
/// ```
pub fn generate_sdk_task_wrapper(
    fn_info: &FnInfo,
    fn_name: &Ident,
    wrapper_name: &Ident,
    input_type: Option<&syn::Type>,
    output_type: Option<&syn::Type>,
) -> TokenStream {
    let ctx_param = if !fn_info.inputs.is_empty() {
        if let syn::FnArg::Typed(pat_type) = &fn_info.inputs[0] {
            if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                Some(&pat_ident.ident)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    if let (Some(input_ty), Some(output_ty), Some(ctx_name)) = (input_type, output_type, ctx_param)
    {
        quote! {
            async fn #wrapper_name(
                #ctx_name: orcher_sdk::TaskContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                use orcher_sdk::payload::{from_payload, to_payload};

                let input: #input_ty = from_payload(&input_payload)?;

                let result: #output_ty = #fn_name(#ctx_name, input).await?;

                let output_payload = to_payload(&result)?;
                Ok(output_payload)
            }
        }
    } else {
        // The signature did not fit; fail when the task runs.
        quote! {
            async fn #wrapper_name(
                ctx: orcher_sdk::TaskContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                Err(orcher_sdk::Error::Other("Task wrapper generation failed".to_string()))
            }
        }
    }
}

/// Generates the payload-level handler for a workflow, without applying any attributes.
///
/// It behaves like [`generate_sdk_task_wrapper`] with a `WorkflowContext`.
#[allow(dead_code)]
pub fn generate_sdk_workflow_wrapper(
    fn_info: &FnInfo,
    fn_name: &Ident,
    wrapper_name: &Ident,
    input_type: Option<&syn::Type>,
    output_type: Option<&syn::Type>,
) -> TokenStream {
    let ctx_param = if !fn_info.inputs.is_empty() {
        if let syn::FnArg::Typed(pat_type) = &fn_info.inputs[0] {
            if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                Some(&pat_ident.ident)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    if let (Some(input_ty), Some(output_ty), Some(ctx_name)) = (input_type, output_type, ctx_param)
    {
        quote! {
            async fn #wrapper_name(
                #ctx_name: orcher_sdk::WorkflowContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                use orcher_sdk::payload::{from_payload, to_payload};

                let input: #input_ty = from_payload(&input_payload)?;

                let result: #output_ty = #fn_name(#ctx_name, input).await?;

                let output_payload = to_payload(&result)?;
                Ok(output_payload)
            }
        }
    } else {
        // The signature did not fit; fail when the workflow runs.
        quote! {
            async fn #wrapper_name(
                ctx: orcher_sdk::WorkflowContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                Err(orcher_sdk::Error::Other("Workflow wrapper generation failed".to_string()))
            }
        }
    }
}

/// Generates the payload-level handler for a workflow, applying its `timeout` and
/// `on_timeout` attributes.
///
/// With `timeout` set, the workflow call is bounded by `tokio::time::timeout` and a timeout
/// returns `Error::Timeout`; `on_timeout = "cancel_with_notification"` also logs an error.
/// `auto_heartbeat` does not send heartbeats: it emits only an empty heartbeat handle and its
/// cleanup.
///
/// # Generated Code Pattern
///
/// The heartbeat task in this example is not part of the generated code.
///
/// ```text
/// async fn my_workflow_handler(
///     ctx: orcher_sdk::WorkflowContext,
///     input_payload: orcher_sdk::Payload,
/// ) -> Result<orcher_sdk::Payload, orcher_sdk::Error> {
///     use orcher_sdk::prelude::*;
///     use orcher_sdk::payload::{from_payload, to_payload};
///
///     // Deserialize the input.
///     let input: MyInput = from_payload(&input_payload)?;
///
///     // Heartbeat task (not generated, see above).
///     let heartbeat_handle = tokio::spawn(async move {
///         let mut interval = tokio::time::interval(Duration::from_secs(30));
///         loop {
///             interval.tick().await;
///             let _ = ctx.heartbeat().await;
///         }
///     });
///
///     // Bound the workflow by its timeout.
///     let result = tokio::time::timeout(
///         Duration::from_secs(600),
///         my_workflow(ctx, input)
///     ).await??;
///
///     // Stop the heartbeat.
///     heartbeat_handle.abort();
///
///     // Serialize the output.
///     Ok(to_payload(&result)?)
/// }
/// ```
pub fn generate_sdk_workflow_wrapper_with_attrs(
    fn_info: &FnInfo,
    fn_name: &Ident,
    wrapper_name: &Ident,
    input_type: Option<&syn::Type>,
    output_type: Option<&syn::Type>,
    attrs: &WorkflowAttrs,
) -> TokenStream {
    let ctx_param = if !fn_info.inputs.is_empty() {
        if let syn::FnArg::Typed(pat_type) = &fn_info.inputs[0] {
            if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                Some(&pat_ident.ident)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    if let (Some(input_ty), Some(output_ty), Some(ctx_name)) = (input_type, output_type, ctx_param)
    {
        let heartbeat_code = if attrs.auto_heartbeat {
            let _interval_secs = attrs.heartbeat_interval.unwrap_or(30);
            quote! {
                // No heartbeat task is started; the handle exists so that cleanup
                // compiles.
                let heartbeat_handle: Option<tokio::task::JoinHandle<()>> = None;
            }
        } else {
            quote! {}
        };

        let heartbeat_cleanup = if attrs.auto_heartbeat {
            quote! {
                if let Some(handle) = heartbeat_handle {
                    handle.abort();
                }
            }
        } else {
            quote! {}
        };

        let workflow_call = if let Some(timeout_secs) = attrs.timeout {
            let on_timeout_handler = match attrs.on_timeout.as_deref() {
                Some("cancel") => quote! {
                    return Err(orcher_sdk::Error::Timeout(format!("Workflow '{}' timed out after {} seconds", stringify!(#fn_name), #timeout_secs)));
                },
                Some("cancel_with_notification") => quote! {
                    tracing::error!("Workflow '{}' timed out after {} seconds - cancelling with notification", stringify!(#fn_name), #timeout_secs);
                    // Notifications are not sent; the timeout is only logged.
                    return Err(orcher_sdk::Error::Timeout(format!("Workflow '{}' timed out after {} seconds", stringify!(#fn_name), #timeout_secs)));
                },
                Some("fail") => quote! {
                    return Err(orcher_sdk::Error::Timeout(format!("Workflow '{}' failed due to timeout after {} seconds", stringify!(#fn_name), #timeout_secs)));
                },
                _ => quote! {
                    return Err(orcher_sdk::Error::Timeout(format!("Workflow '{}' failed due to timeout after {} seconds", stringify!(#fn_name), #timeout_secs)));
                },
            };

            quote! {
                let result: #output_ty = match tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    #fn_name(#ctx_name, input)
                ).await {
                    Ok(Ok(res)) => res,
                    Ok(Err(e)) => {
                        #heartbeat_cleanup
                        return Err(e.into());
                    }
                    Err(_) => {
                        #heartbeat_cleanup
                        #on_timeout_handler
                    }
                };
            }
        } else {
            quote! {
                let result: #output_ty = #fn_name(#ctx_name, input).await?;
            }
        };

        quote! {
            async fn #wrapper_name(
                #ctx_name: orcher_sdk::WorkflowContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                use orcher_sdk::payload::{from_payload, to_payload};

                let input: #input_ty = from_payload(&input_payload)?;

                #heartbeat_code

                #workflow_call

                #heartbeat_cleanup

                let output_payload = to_payload(&result)?;
                Ok(output_payload)
            }
        }
    } else {
        // The signature did not fit; fail when the workflow runs.
        quote! {
            async fn #wrapper_name(
                ctx: orcher_sdk::WorkflowContext,
                input_payload: orcher_sdk::Payload,
            ) -> orcher_sdk::Result<orcher_sdk::Payload> {
                use orcher_sdk::prelude::*;
                Err(orcher_sdk::Error::Other("Workflow wrapper generation failed".to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_capitalize_first() {
        assert_eq!(capitalize_first("hello"), "Hello");
        assert_eq!(capitalize_first("world"), "World");
        assert_eq!(capitalize_first(""), "");
        assert_eq!(capitalize_first("a"), "A");
    }

    #[test]
    fn test_task_wrapper_name() {
        let fn_name: Ident = parse_quote!(process_data);
        let wrapper = task_wrapper_name(&fn_name);
        assert_eq!(wrapper.to_string(), "Process_dataTask");
    }

    #[test]
    fn test_workflow_wrapper_name() {
        let fn_name: Ident = parse_quote!(data_pipeline);
        let wrapper = workflow_wrapper_name(&fn_name);
        assert_eq!(wrapper.to_string(), "Data_pipelineWorkflow");
    }
}
