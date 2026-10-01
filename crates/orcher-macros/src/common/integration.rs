//! Helpers for recognizing SDK types (`TaskContext`, `WorkflowContext`, `Result`) in user
//! function signatures.
//!
//! Proc macros see only tokens, so types are recognized by the last segment of their path:
//! `TaskContext` and `orcher::TaskContext` both match, and so does any other type with that
//! name.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, ReturnType, Type, TypePath};

use crate::common::codegen::FnInfo;

/// Returns whether the type's path ends in `TaskContext`.
///
/// # Examples
///
/// ```text
/// let ty: Type = parse_quote!(TaskContext);
/// assert!(is_task_context_type(&ty));
///
/// let ty: Type = parse_quote!(orcher::TaskContext);
/// assert!(is_task_context_type(&ty));
/// ```
pub fn is_task_context_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            if segment.ident == "TaskContext" {
                return true;
            }
        }
    }
    false
}

/// Returns whether the type's path ends in `WorkflowContext`.
pub fn is_workflow_context_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            if segment.ident == "WorkflowContext" {
                return true;
            }
        }
    }
    false
}

/// Returns whether the function's first parameter is a `TaskContext`.
///
/// # Examples
///
/// ```text
/// // true
/// async fn my_task(ctx: TaskContext, input: String) -> Result<()>
///
/// // false
/// async fn my_task(input: String) -> Result<()>
/// ```
pub fn uses_sdk_task_context(fn_info: &FnInfo) -> bool {
    if let Some(FnArg::Typed(pat_type)) = fn_info.inputs.first() {
        return is_task_context_type(&pat_type.ty);
    }
    false
}

/// Returns whether the function's first parameter is a `WorkflowContext`.
pub fn uses_sdk_workflow_context(fn_info: &FnInfo) -> bool {
    if let Some(FnArg::Typed(pat_type)) = fn_info.inputs.first() {
        return is_workflow_context_type(&pat_type.ty);
    }
    false
}

/// Returns the input type: the second parameter, when the first is a task or workflow
/// context.
///
/// # Examples
///
/// ```text
/// // For: async fn my_task(ctx: TaskContext, input: MyInput) -> Result<Output>
/// // Returns: Some(Type representing MyInput)
///
/// // For: async fn my_task(input: MyInput) -> Result<Output>
/// // Returns: None (no context parameter)
/// ```
pub fn extract_sdk_input_type(fn_info: &FnInfo) -> Option<&Type> {
    if (uses_sdk_task_context(fn_info) || uses_sdk_workflow_context(fn_info))
        && fn_info.inputs.len() >= 2
    {
        if let FnArg::Typed(pat_type) = &fn_info.inputs[1] {
            return Some(&pat_type.ty);
        }
    }
    None
}

/// Returns `T` from a `Result<T>` or `Result<T, E>` return type, or `None` for anything else.
///
/// # Examples
///
/// ```text
/// // For: Result<MyOutput, Error>
/// // Returns: Some(Type representing MyOutput)
///
/// // For: Result<MyOutput>
/// // Returns: Some(Type representing MyOutput)
///
/// // For: MyOutput (not a Result)
/// // Returns: None
/// ```
pub fn extract_sdk_output_type(output: &ReturnType) -> Option<&Type> {
    if let ReturnType::Type(_, ty) = output {
        if let Type::Path(TypePath { path, .. }) = ty.as_ref() {
            if let Some(segment) = path.segments.last() {
                if segment.ident == "Result" {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                            return Some(inner_ty);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Returns whether the type's path ends in `Result`.
#[allow(dead_code)]
pub fn is_result_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            return segment.ident == "Result";
        }
    }
    false
}

/// Generates `use orcher::prelude::*;`.
#[allow(dead_code)]
pub fn generate_sdk_prelude_import() -> TokenStream {
    quote! {
        use orcher::prelude::*;
    }
}

/// Generates `use orcher::payload::{to_payload, from_payload};`.
#[allow(dead_code)]
pub fn generate_payload_utils_import() -> TokenStream {
    quote! {
        use orcher::payload::{to_payload, from_payload};
    }
}

/// Returns whether the first parameter is a task or workflow context.
#[allow(dead_code)]
pub fn has_sdk_context(fn_info: &FnInfo) -> bool {
    uses_sdk_task_context(fn_info) || uses_sdk_workflow_context(fn_info)
}

/// Returns the name of the first parameter, usually the context.
#[allow(dead_code)]
pub fn extract_first_param_name(fn_info: &FnInfo) -> Option<&syn::Ident> {
    if let Some(FnArg::Typed(pat_type)) = fn_info.inputs.first() {
        if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
            return Some(&pat_ident.ident);
        }
    }
    None
}

/// Returns the name of the second parameter, usually the input.
#[allow(dead_code)]
pub fn extract_second_param_name(fn_info: &FnInfo) -> Option<&syn::Ident> {
    if fn_info.inputs.len() >= 2 {
        if let FnArg::Typed(pat_type) = &fn_info.inputs[1] {
            if let syn::Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                return Some(&pat_ident.ident);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::{parse_quote, ItemFn};

    use crate::common::codegen::extract_fn_info;

    #[test]
    fn test_is_task_context_type() {
        let ty: Type = parse_quote!(TaskContext);
        assert!(is_task_context_type(&ty));

        let ty: Type = parse_quote!(orcher::TaskContext);
        assert!(is_task_context_type(&ty));

        let ty: Type = parse_quote!(String);
        assert!(!is_task_context_type(&ty));
    }

    #[test]
    fn test_is_workflow_context_type() {
        let ty: Type = parse_quote!(WorkflowContext);
        assert!(is_workflow_context_type(&ty));

        let ty: Type = parse_quote!(orcher::WorkflowContext);
        assert!(is_workflow_context_type(&ty));

        let ty: Type = parse_quote!(String);
        assert!(!is_workflow_context_type(&ty));
    }

    #[test]
    fn test_uses_sdk_task_context() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: String) -> Result<String> {
                Ok(input)
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        assert!(uses_sdk_task_context(&fn_info));
    }

    #[test]
    fn test_uses_sdk_task_context_false() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_task(input: String) -> Result<String> {
                Ok(input)
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        assert!(!uses_sdk_task_context(&fn_info));
    }

    #[test]
    fn test_uses_sdk_workflow_context() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_workflow(ctx: WorkflowContext, input: String) -> Result<String> {
                Ok(input)
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        assert!(uses_sdk_workflow_context(&fn_info));
    }

    #[test]
    fn test_extract_sdk_input_type() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: MyInput) -> Result<MyOutput> {
                Ok(MyOutput {})
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        let input_type = extract_sdk_input_type(&fn_info);
        assert!(input_type.is_some());
    }

    #[test]
    fn test_extract_sdk_output_type() {
        let return_type: ReturnType = parse_quote! {
            -> Result<MyOutput, Error>
        };
        let output_type = extract_sdk_output_type(&return_type);
        assert!(output_type.is_some());
    }

    #[test]
    fn test_is_result_type() {
        let ty: Type = parse_quote!(Result<String, Error>);
        assert!(is_result_type(&ty));

        let ty: Type = parse_quote!(Result<String>);
        assert!(is_result_type(&ty));

        let ty: Type = parse_quote!(String);
        assert!(!is_result_type(&ty));
    }

    #[test]
    fn test_has_sdk_context() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: String) -> Result<String> {
                Ok(input)
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        assert!(has_sdk_context(&fn_info));

        let item_fn: ItemFn = parse_quote! {
            async fn my_task(input: String) -> Result<String> {
                Ok(input)
            }
        };
        let fn_info = extract_fn_info(&item_fn);
        assert!(!has_sdk_context(&fn_info));
    }

    #[test]
    fn test_extract_param_names() {
        let item_fn: ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: MyInput) -> Result<MyOutput> {
                Ok(MyOutput {})
            }
        };
        let fn_info = extract_fn_info(&item_fn);

        let first_name = extract_first_param_name(&fn_info);
        assert!(first_name.is_some());
        assert_eq!(first_name.unwrap().to_string(), "ctx");

        let second_name = extract_second_param_name(&fn_info);
        assert!(second_name.is_some());
        assert_eq!(second_name.unwrap().to_string(), "input");
    }
}
