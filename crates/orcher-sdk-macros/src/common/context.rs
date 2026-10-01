//! JSON-based task wrappers with a type-erased context.
//!
//! These wrappers take the context as `&dyn Any` and the input as a `serde_json::Value`.
//! `#[task]` only selects them for functions without the SDK `TaskContext`, and it rejects
//! such functions before registration, so the SDK wrapper in `codegen` is the one in use.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ItemFn};

use crate::common::codegen::FnInfo;

/// Generates a wrapper that downcasts the context to `orcher_sdk::TaskContext`.
///
/// The wrapper deserializes the input, calls the task function, and serializes the result
/// to JSON. A single input parameter is decoded from the whole input value; several are
/// decoded from the fields of a JSON object, by parameter name.
/// # Example
///
/// For a user function:
/// ```text
/// #[task]
/// async fn my_task(ctx: TaskContext, input: MyInput) -> Result<MyOutput> {
///     // ...
/// }
/// ```
///
/// Generates:
/// ```text
/// fn my_task_wrapper(
///     context_any: &(dyn std::any::Any + Send + Sync),
///     input_json: serde_json::Value,
/// ) -> Pin<Box<dyn Future<Output = Result<Value, Box<dyn Error + Send + Sync>>> + Send>> {
///     // The context is cloned out so the future owns it.
///     let ctx = context_any.downcast_ref::<TaskContext>().cloned();
///
///     Box::pin(async move {
///         let ctx = ctx.ok_or("Failed to downcast to TaskContext")?;
///         let input: MyInput = serde_json::from_value(input_json)?;
///         let result = my_task(ctx, input).await?;
///         Ok(serde_json::to_value(result)?)
///     })
/// }
/// ```
pub fn generate_task_context_wrapper(
    fn_info: &FnInfo,
    _item_fn: &ItemFn,
    wrapper_name: &Ident,
    fn_impl_name: &Ident,
) -> TokenStream {
    let fn_name = fn_impl_name;

    // Every parameter after the context is a task input.
    let input_params: Vec<_> = fn_info
        .inputs
        .iter()
        .skip(1) // Skip the context parameter.
        .collect();

    let has_input = !input_params.is_empty();

    let input_deserialization = if has_input {
        if input_params.len() == 1 {
            if let syn::FnArg::Typed(pat_type) = input_params[0] {
                let input_type = &pat_type.ty;
                quote! {
                    let input: #input_type = ::orcher_sdk::__private::serde_json::from_value(input_json)
                        .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;
                }
            } else {
                quote! {
                    let input = input_json;
                }
            }
        } else {
            // Several inputs are read from a JSON object keyed by parameter name.
            let param_deserializations: Vec<_> = input_params
                .iter()
                .filter_map(|param| {
                    if let syn::FnArg::Typed(pat_type) = param {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            let param_name = &pat_ident.ident;
                            let param_name_str = param_name.to_string();
                            let param_type = &pat_type.ty;
                            return Some(quote! {
                                let #param_name: #param_type = ::orcher_sdk::__private::serde_json::from_value(
                                    input_json.get(#param_name_str)
                                        .ok_or_else(|| format!("Missing parameter: {}", #param_name_str))?
                                        .clone()
                                ).map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;
                            });
                        }
                    }
                    None
                })
                .collect();

            quote! {
                #(#param_deserializations)*
            }
        }
    } else {
        quote! {}
    };

    let call_args = if has_input {
        if input_params.len() == 1 {
            quote! { ctx, input }
        } else {
            let param_names: Vec<_> = input_params
                .iter()
                .filter_map(|param| {
                    if let syn::FnArg::Typed(pat_type) = param {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            quote! { ctx, #(#param_names),* }
        }
    } else {
        quote! { ctx }
    };

    quote! {
        /// Wrapper that runs the task with an `orcher_sdk::TaskContext` taken from a type-erased
        /// context, converting input and output through JSON.
        fn #wrapper_name(
            context_any: &(dyn ::std::any::Any + Send + Sync),
            input_json: ::orcher_sdk::__private::serde_json::Value,
        ) -> ::std::pin::Pin<::std::boxed::Box<
            dyn ::std::future::Future<
                Output = ::std::result::Result<
                    ::orcher_sdk::__private::serde_json::Value,
                    Box<dyn ::std::error::Error + Send + Sync>
                >
            > + Send
        >> {
            // Clone the context before building the future, which cannot borrow
            // `context_any`.
            let ctx_result = context_any
                .downcast_ref::<orcher_sdk::TaskContext>()
                .ok_or_else(|| {
                    Box::<dyn ::std::error::Error + Send + Sync>::from(
                        "Failed to downcast context to TaskContext. \
                         Ensure you're using orcher SDK's TaskContext type."
                    )
                })
                .map(|c| c.clone());

            Box::pin(async move {
                let ctx = ctx_result?;

                #input_deserialization

                let result = #fn_name(#call_args).await
                    .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;

                let output = ::orcher_sdk::__private::serde_json::to_value(result)
                    .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;

                Ok(output)
            })
        }
    }
}

/// Generates a wrapper for a task function that takes no context.
///
/// Inputs are decoded as in [`generate_task_context_wrapper`] and the context argument is
/// ignored.
pub fn generate_legacy_wrapper(
    fn_info: &FnInfo,
    _item_fn: &ItemFn,
    wrapper_name: &Ident,
    fn_impl_name: &Ident,
) -> TokenStream {
    let fn_name = fn_impl_name;

    let input_params: Vec<_> = fn_info.inputs.iter().collect();

    let has_input = !input_params.is_empty();

    let input_deserialization = if has_input {
        if input_params.len() == 1 {
            if let syn::FnArg::Typed(pat_type) = input_params[0] {
                let input_type = &pat_type.ty;
                quote! {
                    let input: #input_type = ::orcher_sdk::__private::serde_json::from_value(input_json)
                        .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;
                }
            } else {
                quote! {}
            }
        } else {
            let param_deserializations: Vec<_> = input_params
                .iter()
                .filter_map(|param| {
                    if let syn::FnArg::Typed(pat_type) = param {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            let param_name = &pat_ident.ident;
                            let param_name_str = param_name.to_string();
                            let param_type = &pat_type.ty;
                            return Some(quote! {
                                let #param_name: #param_type = ::orcher_sdk::__private::serde_json::from_value(
                                    input_json.get(#param_name_str)
                                        .ok_or_else(|| format!("Missing parameter: {}", #param_name_str))?
                                        .clone()
                                ).map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;
                            });
                        }
                    }
                    None
                })
                .collect();

            quote! {
                #(#param_deserializations)*
            }
        }
    } else {
        quote! {}
    };

    let call_args = if has_input {
        if input_params.len() == 1 {
            quote! { input }
        } else {
            let param_names: Vec<_> = input_params
                .iter()
                .filter_map(|param| {
                    if let syn::FnArg::Typed(pat_type) = param {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some(&pat_ident.ident);
                        }
                    }
                    None
                })
                .collect();
            quote! { #(#param_names),* }
        }
    } else {
        quote! {}
    };

    quote! {
        /// Wrapper that runs a context-free task, converting input and output through JSON.
        ///
        /// It returns a boxed future rather than being an `async fn` so that the future does
        /// not capture the lifetime of the `&dyn Any` parameter.
        fn #wrapper_name(
            _context_any: &(dyn ::std::any::Any + Send + Sync),
            input_json: ::orcher_sdk::__private::serde_json::Value,
        ) -> ::std::pin::Pin<::std::boxed::Box<
            dyn ::std::future::Future<
                Output = ::std::result::Result<
                    ::orcher_sdk::__private::serde_json::Value,
                    Box<dyn ::std::error::Error + Send + Sync>
                >
            > + Send
        >> {
            Box::pin(async move {
                #input_deserialization

                let result = #fn_name(#call_args).await
                    .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;

                let output = ::orcher_sdk::__private::serde_json::to_value(result)
                    .map_err(|e| Box::new(e) as Box<dyn ::std::error::Error + Send + Sync>)?;

                Ok(output)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    #[test]
    fn test_task_context_detection() {
        let item_fn: syn::ItemFn = parse_quote! {
            async fn my_task(ctx: TaskContext, input: String) -> Result<String> {
                Ok(input)
            }
        };

        let fn_info = crate::common::codegen::extract_fn_info(&item_fn);
        assert!(crate::common::codegen::uses_task_context(&fn_info));
    }

    #[test]
    fn test_legacy_task_detection() {
        let item_fn: syn::ItemFn = parse_quote! {
            async fn my_task(input: String) -> Result<String> {
                Ok(input)
            }
        };

        let fn_info = crate::common::codegen::extract_fn_info(&item_fn);
        assert!(!crate::common::codegen::uses_task_context(&fn_info));
    }
}
