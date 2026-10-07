//! Code generation helpers shared by the task and workflow macros.

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

/// Generates the payload-level handler for a workflow.
///
/// It behaves like [`generate_sdk_task_wrapper`] with a `WorkflowContext`. No workflow
/// attribute changes it: a workflow runs one activation at a time and suspends between
/// them, so a wall-clock bound placed here would limit a single activation, not the
/// workflow, and would make replay depend on timing.
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
