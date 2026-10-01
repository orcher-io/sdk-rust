//! Implementation of the `#[query]` attribute macro.
//!
//! A query is a synchronous handler called on a running workflow to inspect its state
//! without affecting execution. The macro keeps the original function and generates a
//! `<Name>Query` struct that exposes the handler's configuration and an `execute` method
//! forwarding to it.

use crate::common::attrs::{validate_query_attrs, QueryAttrs};
use crate::common::codegen::extract_fn_info;
use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Meta};

/// Entry point for `#[query]`: parses the attribute arguments and expands the handler.
pub fn query_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as ItemFn);

    let attrs = if attr.is_empty() {
        QueryAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();

        // Wrap the arguments as `query(...)` so darling can parse them as a list.
        let wrapped_tokens = quote::quote! { query(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match QueryAttrs::from_meta(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse query attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match query_impl_inner(attrs, item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn query_impl_inner(attrs: QueryAttrs, item_fn: ItemFn) -> Result<TokenStream2, syn::Error> {
    let fn_info = extract_fn_info(&item_fn);

    validate_query_attrs(&attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid query configuration: {}", e),
        )
    })?;

    let fn_name = &fn_info.name;
    let fn_inputs = &fn_info.inputs;
    let fn_output = &fn_info.output;
    let fn_body = &item_fn.block;
    let fn_vis = &item_fn.vis;
    let fn_attrs = &item_fn.attrs;

    if fn_info.is_async {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "Query functions must NOT be async (they should be fast, read-only operations)",
        ));
    }

    if matches!(fn_output, syn::ReturnType::Default) {
        return Err(syn::Error::new_spanned(
            &item_fn.sig,
            "Query functions must have a return type",
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

    let wrapper_name = syn::Ident::new(
        &format!("{}Query", capitalize_first(&fn_name.to_string())),
        fn_name.span(),
    );

    let query_name = attrs
        .name
        .as_ref()
        .map(|n| n.to_string())
        .unwrap_or_else(|| fn_name.to_string());

    let description = attrs
        .description
        .as_ref()
        .map(|d| quote! { Some(#d.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let timeout_seconds = attrs
        .timeout
        .map(|t| quote! { Some(#t) })
        .unwrap_or_else(|| quote! { None });

    let namespace = attrs
        .namespace
        .as_ref()
        .map(|ns| quote! { Some(#ns.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let cache_ttl_seconds = attrs
        .cache_ttl
        .map(|ttl| quote! { Some(#ttl) })
        .unwrap_or_else(|| quote! { None });

    let version = attrs
        .version
        .as_ref()
        .map(|v| quote! { Some(#v.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let readonly = attrs.readonly;

    let expanded = quote! {
        #(#fn_attrs)*
        #fn_vis fn #fn_name(#fn_inputs) #fn_output {
            #fn_body
        }

        #[automatically_derived]
        #[doc = concat!("Query wrapper for `", stringify!(#fn_name), "`")]
        #[allow(non_camel_case_types)]
        pub struct #wrapper_name;

        #[automatically_derived]
        impl #wrapper_name {
            /// Returns the query name (the function name unless `name` was set).
            pub fn name() -> &'static str {
                #query_name
            }

            /// Returns the query description, if one was configured.
            pub fn description() -> Option<String> {
                #description
            }

            /// Returns the query timeout in seconds, if one was configured.
            pub fn timeout_seconds() -> Option<u64> {
                #timeout_seconds
            }

            /// Returns the namespace, if one was configured.
            pub fn namespace() -> Option<String> {
                #namespace
            }

            /// Returns how long a result may be cached, in seconds, if configured.
            pub fn cache_ttl_seconds() -> Option<u64> {
                #cache_ttl_seconds
            }

            /// Returns the query version, if one was configured.
            pub fn version() -> Option<String> {
                #version
            }

            /// Returns whether the query is declared read-only (the default).
            pub fn is_readonly() -> bool {
                #readonly
            }

            /// Runs the query.
            pub fn execute(#fn_inputs) #fn_output {
                #fn_name(#(#input_names),*)
            }
        }
    };

    Ok(expanded)
}

/// Uppercases the first character of `s`.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use darling::FromMeta;
    use syn::parse_quote;

    #[test]
    fn test_query_impl_basic() {
        let meta: Meta = parse_quote!(query(name = "test_query"));
        let attrs = QueryAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            fn my_query() -> String {
                "status".to_string()
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_impl_with_description() {
        let meta: Meta = parse_quote!(query(name = "test_query", description = "A test query"));
        let attrs = QueryAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            fn my_query() -> String {
                "status".to_string()
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_impl_async_fails() {
        let attrs = QueryAttrs::default();
        let item: ItemFn = parse_quote! {
            async fn my_query() -> String {
                "status".to_string()
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must NOT be async"));
    }

    #[test]
    fn test_query_impl_no_return_fails() {
        let attrs = QueryAttrs::default();
        let item: ItemFn = parse_quote! {
            fn my_query() {
                println!("query");
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must have a return type"));
    }

    #[test]
    fn test_query_impl_with_timeout() {
        let meta: Meta = parse_quote!(query(name = "test_query", timeout = 30));
        let attrs = QueryAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            fn my_query() -> String {
                "status".to_string()
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_impl_with_cache_ttl() {
        let meta: Meta = parse_quote!(query(name = "test_query", cache_ttl = 300));
        let attrs = QueryAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.cache_ttl, Some(300));
    }

    #[test]
    fn test_query_impl_with_namespace() {
        let meta: Meta = parse_quote!(query(name = "test_query", namespace = "production"));
        let attrs = QueryAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.namespace, Some("production".to_string()));

        let item: ItemFn = parse_quote! {
            fn my_query() -> String {
                "status".to_string()
            }
        };

        let result = query_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_impl_readonly_default() {
        let attrs = QueryAttrs::default();
        assert!(attrs.readonly);
    }

    #[test]
    fn test_capitalize_first() {
        assert_eq!(capitalize_first("hello"), "Hello");
        assert_eq!(capitalize_first("world"), "World");
        assert_eq!(capitalize_first(""), "");
        assert_eq!(capitalize_first("a"), "A");
    }
}
