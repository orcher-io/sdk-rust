//! Code generation helpers for the actor macros.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ImplItem, ImplItemFn, ItemImpl, ItemStruct, ReturnType, Type};

/// Returns the struct's identifier.
pub fn extract_struct_name(item: &ItemStruct) -> syn::Ident {
    item.ident.clone()
}

/// Returns the actor name: the `name` argument if given, otherwise the struct name.
pub fn determine_actor_name(
    attrs: &crate::actor::attrs::ActorAttrs,
    struct_name: &syn::Ident,
) -> String {
    attrs
        .name
        .clone()
        .unwrap_or_else(|| struct_name.to_string())
}

/// Generates the `orcher::actor::Actor` implementation for the struct.
pub fn generate_actor_trait_impl(struct_name: &syn::Ident, actor_name: &str) -> TokenStream {
    quote! {
        impl ::orcher::actor::Actor for #struct_name {
            fn actor_name() -> &'static str {
                #actor_name
            }
        }
    }
}

/// Generates the `ACTOR_NAME` associated constant.
pub fn generate_actor_name_constant(struct_name: &syn::Ident, actor_name: &str) -> TokenStream {
    quote! {
        impl #struct_name {
            /// The actor type name.
            pub const ACTOR_NAME: &'static str = #actor_name;
        }
    }
}

/// An `#[operation]` method, as parsed from the `#[operations]` impl block.
#[derive(Debug, Clone)]
pub struct OperationInfo {
    /// Method name, also used as the operation name.
    pub name: syn::Ident,
    /// Concurrency mode.
    pub mode: crate::actor::attrs::OperationModeAttr,
    /// Parameters after the context, as name and type.
    pub inputs: Vec<(syn::Ident, Type)>,
    /// Declared return type.
    pub output: ReturnType,
    /// Whether the method is async (always true after validation).
    #[allow(dead_code)]
    pub is_async: bool,
    /// Method visibility.
    #[allow(dead_code)]
    pub vis: syn::Visibility,
}

/// Collects every method marked `#[operation(...)]`; other methods are skipped.
///
/// # Errors
///
/// Returns an error if an operation attribute or signature is invalid.
pub fn extract_operations(impl_block: &ItemImpl) -> syn::Result<Vec<OperationInfo>> {
    let mut operations = Vec::new();

    for item in &impl_block.items {
        if let ImplItem::Fn(method) = item {
            let mut mode = None;

            for attr in &method.attrs {
                if let Some(m) = crate::actor::attrs::parse_operation_mode(attr)? {
                    mode = Some(m);
                    break;
                }
            }

            let Some(operation_mode) = mode else {
                continue;
            };

            let info = extract_operation_info(method, operation_mode)?;
            operations.push(info);
        }
    }

    Ok(operations)
}

/// Parses and validates one operation method.
///
/// An operation must be async and take `ActorContext` or `SharedActorContext` as its first
/// parameter. Every other parameter must be a plain identifier.
fn extract_operation_info(
    method: &ImplItemFn,
    mode: crate::actor::attrs::OperationModeAttr,
) -> syn::Result<OperationInfo> {
    let name = method.sig.ident.clone();
    let is_async = method.sig.asyncness.is_some();
    let vis = method.vis.clone();

    if !is_async {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "Actor operations must be async",
        ));
    }

    let mut inputs = Vec::new();
    let mut iter = method.sig.inputs.iter();

    if let Some(FnArg::Typed(pat_type)) = iter.next() {
        // Matched on the type's tokens, so any path ending in either name is accepted.
        let type_str = quote!(#pat_type.ty).to_string();
        if !type_str.contains("ActorContext") && !type_str.contains("SharedActorContext") {
            return Err(syn::Error::new_spanned(
                pat_type,
                "First parameter must be ActorContext or SharedActorContext",
            ));
        }
    } else {
        return Err(syn::Error::new_spanned(
            &method.sig.inputs,
            "Actor operations must have a context parameter as the first argument",
        ));
    }

    for arg in iter {
        if let FnArg::Typed(pat_type) = arg {
            if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                inputs.push((pat_ident.ident.clone(), (*pat_type.ty).clone()));
            } else {
                return Err(syn::Error::new_spanned(
                    arg,
                    "Operation parameters must be simple identifiers",
                ));
            }
        }
    }

    let output = method.sig.output.clone();

    Ok(OperationInfo {
        name,
        mode,
        inputs,
        output,
        is_async,
        vis,
    })
}

/// Generates the `operations()` method that lists each operation's name and mode.
pub fn generate_operations_metadata(
    struct_name: &syn::Ident,
    operations: &[OperationInfo],
) -> TokenStream {
    let operation_metas = operations.iter().map(|op| {
        let name = op.name.to_string();
        let mode = match op.mode {
            crate::actor::attrs::OperationModeAttr::Exclusive => {
                quote! { ::orcher::actor::types::OperationMode::Exclusive }
            }
            crate::actor::attrs::OperationModeAttr::Shared => {
                quote! { ::orcher::actor::types::OperationMode::Shared }
            }
        };

        quote! {
            ::orcher::actor::types::OperationMetadata {
                actor_name: Self::ACTOR_NAME.to_string(),
                operation_name: #name.to_string(),
                mode: #mode,
            }
        }
    });

    quote! {
        impl #struct_name {
            /// Returns metadata for every operation on this actor.
            pub fn operations() -> Vec<::orcher::actor::types::OperationMetadata> {
                vec![
                    #(#operation_metas),*
                ]
            }
        }
    }
}

/// Returns a copy of the method renamed to `<name>_impl`, without its `#[operation]`
/// attribute.
pub fn rename_impl_method(method: &ImplItemFn) -> ImplItemFn {
    let mut renamed = method.clone();
    let original_name = &method.sig.ident;
    let impl_name = format_ident!("{}_impl", original_name);
    renamed.sig.ident = impl_name;

    renamed
        .attrs
        .retain(|attr| !attr.path().is_ident("operation"));

    renamed
}

/// Extracts `T` from a `Result<T>` or `Result<T, E>` return type.
///
/// Any other type is returned unchanged, and a missing return type yields `()`.
fn extract_inner_result_type(output: &ReturnType) -> TokenStream {
    if let ReturnType::Type(_, ty) = output {
        if let Type::Path(type_path) = &**ty {
            if let Some(segment) = type_path.path.segments.last() {
                if segment.ident == "Result" {
                    if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                            return quote! { #inner_ty };
                        }
                    }
                }
            }
        }
        quote! { #ty }
    } else {
        quote! { () }
    }
}

/// Generates a typed client struct for the actor and its `ActorRef` implementation.
///
/// Each operation gets a method that waits for the result and a `send_` method that does
/// not. For an actor `Counter` with operations `increment(delta: i64) -> Result<i64>` and
/// `get_count() -> Result<i64>`, the output is:
///
/// ```text
/// #[derive(Clone)]
/// pub struct CounterClient {
///     inner: ::orcher::actor::ActorInvocationClient,
/// }
///
/// impl CounterClient {
///     pub async fn increment(&self, delta: i64) -> ::orcher::error::Result<i64> { ... }
///     pub async fn send_increment(&self, delta: i64) -> ::orcher::error::Result<()> { ... }
///     pub async fn get_count(&self) -> ::orcher::error::Result<i64> { ... }
///     pub async fn send_get_count(&self) -> ::orcher::error::Result<()> { ... }
/// }
///
/// impl ::orcher::actor::ActorRef for Counter {
///     type Client = CounterClient;
///     fn client(handle: ::orcher::actor::ActorInvocationClient) -> CounterClient { ... }
/// }
/// ```
pub fn generate_typed_client(
    struct_name: &syn::Ident,
    operations: &[OperationInfo],
) -> TokenStream {
    let client_name = format_ident!("{}Client", struct_name);

    let call_methods: Vec<TokenStream> = operations
        .iter()
        .map(|op| {
            let method_name = &op.name;
            let method_name_str = method_name.to_string();
            let send_method_name = format_ident!("send_{}", method_name);
            let return_type = extract_inner_result_type(&op.output);

            let param_names: Vec<&syn::Ident> = op.inputs.iter().map(|(name, _)| name).collect();
            let param_types: Vec<&Type> = op.inputs.iter().map(|(_, ty)| ty).collect();

            let params: Vec<TokenStream> = op
                .inputs
                .iter()
                .map(|(name, ty)| quote! { #name: #ty })
                .collect();

            // Arguments are always sent as a tuple, because the server-side handler
            // deserializes its input as one.
            let (input_type, input_value) = if param_types.is_empty() {
                (quote! { () }, quote! { &() })
            } else if param_types.len() == 1 {
                let ty = &param_types[0];
                let name = &param_names[0];
                (quote! { (#ty,) }, quote! { &(#name,) })
            } else {
                (
                    quote! { (#(#param_types,)*) },
                    quote! { &(#(#param_names,)*) },
                )
            };

            quote! {
                /// Invokes this operation and waits for the result.
                pub async fn #method_name(&self, #(#params),*) -> ::orcher::error::Result<#return_type> {
                    self.inner.invoke::<#input_type, #return_type>(#method_name_str, #input_value, None).await
                }

                /// Invokes this operation without waiting for the result.
                pub async fn #send_method_name(&self, #(#params),*) -> ::orcher::error::Result<()> {
                    self.inner.send::<#input_type>(#method_name_str, #input_value, None).await
                }
            }
        })
        .collect();

    quote! {
        /// Typed client for invoking this actor's operations.
        ///
        /// Generated by `#[operations]`. Obtain one with `client.actor::<Actor>(key)`.
        #[derive(Clone)]
        pub struct #client_name {
            /// The underlying untyped invocation handle.
            pub inner: ::orcher::actor::ActorInvocationClient,
        }

        impl #client_name {
            #(#call_methods)*
        }

        impl ::orcher::actor::ActorRef for #struct_name {
            type Client = #client_name;

            fn client(handle: ::orcher::actor::ActorInvocationClient) -> #client_name {
                #client_name { inner: handle }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_actor_name_from_attr() {
        let attrs = crate::actor::attrs::ActorAttrs {
            name: Some("CustomName".to_string()),
        };
        let struct_name = syn::parse_str("MyActor").unwrap();
        assert_eq!(determine_actor_name(&attrs, &struct_name), "CustomName");
    }

    #[test]
    fn test_determine_actor_name_from_struct() {
        let attrs = crate::actor::attrs::ActorAttrs { name: None };
        let struct_name = syn::parse_str("MyActor").unwrap();
        assert_eq!(determine_actor_name(&attrs, &struct_name), "MyActor");
    }
}
