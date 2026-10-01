//! Implementation of the `#[event]` attribute macro.
//!
//! An event is a message sent to a running workflow to trigger an action or a state change.
//! Unlike a query, an event handler is async and may modify workflow state. The macro keeps
//! the original function and generates a `<Name>Event` struct that exposes the handler's
//! configuration and an `execute` method forwarding to it.

use crate::common::attrs::{validate_event_attrs, EventAttrs};
use crate::common::codegen::extract_fn_info;
use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Meta};

/// Entry point for `#[event]`: parses the attribute arguments and expands the handler.
pub fn event_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as ItemFn);

    let attrs = if attr.is_empty() {
        EventAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();

        // Wrap the arguments as `event(...)` so darling can parse them as a list.
        let wrapped_tokens = quote::quote! { event(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match EventAttrs::from_meta(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse event attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match event_impl_inner(attrs, item_fn) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn event_impl_inner(attrs: EventAttrs, item_fn: ItemFn) -> Result<TokenStream2, syn::Error> {
    let fn_info = extract_fn_info(&item_fn);

    validate_event_attrs(&attrs).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid event configuration: {}", e),
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
            "Event handlers must be async (events can take time and modify state)",
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
        &format!("{}Event", capitalize_first(&fn_name.to_string())),
        fn_name.span(),
    );

    let event_name = attrs
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

    let version = attrs
        .version
        .as_ref()
        .map(|v| quote! { Some(#v.to_string()) })
        .unwrap_or_else(|| quote! { None });

    let max_queue_size = attrs
        .max_queue_size
        .map(|mqs| quote! { Some(#mqs) })
        .unwrap_or_else(|| quote! { None });

    let priority = attrs
        .priority
        .map(|p| quote! { Some(#p) })
        .unwrap_or_else(|| quote! { None });

    let expanded = quote! {
        #(#fn_attrs)*
        #fn_vis async fn #fn_name(#fn_inputs) #fn_output {
            #fn_body
        }

        #[automatically_derived]
        #[doc = concat!("Event handler wrapper for `", stringify!(#fn_name), "`")]
        #[allow(non_camel_case_types)]
        pub struct #wrapper_name;

        #[automatically_derived]
        impl #wrapper_name {
            /// Returns the event name (the function name unless `name` was set).
            pub fn name() -> &'static str {
                #event_name
            }

            /// Returns the event description, if one was configured.
            pub fn description() -> Option<String> {
                #description
            }

            /// Returns the handler timeout in seconds, if one was configured.
            pub fn timeout_seconds() -> Option<u64> {
                #timeout_seconds
            }

            /// Returns the namespace, if one was configured.
            pub fn namespace() -> Option<String> {
                #namespace
            }

            /// Returns the event version, if one was configured.
            pub fn version() -> Option<String> {
                #version
            }

            /// Returns the maximum number of queued events, if configured.
            pub fn max_queue_size() -> Option<u32> {
                #max_queue_size
            }

            /// Returns the event priority (0-100, higher is more important), if configured.
            pub fn priority() -> Option<u8> {
                #priority
            }

            /// Runs the event handler.
            pub async fn execute(#fn_inputs) #fn_output {
                #fn_name(#(#input_names),*).await
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
    fn test_event_impl_basic() {
        let meta: Meta = parse_quote!(event(name = "test_event"));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_event() -> Result<()> {
                Ok(())
            }
        };

        let result = event_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_impl_with_description() {
        let meta: Meta = parse_quote!(event(
            name = "test_event",
            description = "A test event handler"
        ));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_event() -> Result<()> {
                Ok(())
            }
        };

        let result = event_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_impl_non_async_fails() {
        let attrs = EventAttrs::default();
        let item: ItemFn = parse_quote! {
            fn my_event() -> Result<()> {
                Ok(())
            }
        };

        let result = event_impl_inner(attrs, item);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be async"));
    }

    #[test]
    fn test_event_impl_with_timeout() {
        let meta: Meta = parse_quote!(event(name = "test_event", timeout = 30));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        let item: ItemFn = parse_quote! {
            async fn my_event() -> Result<()> {
                Ok(())
            }
        };

        let result = event_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_impl_with_priority() {
        let meta: Meta = parse_quote!(event(name = "test_event", priority = 80));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.priority, Some(80));
    }

    #[test]
    fn test_event_impl_with_namespace() {
        let meta: Meta = parse_quote!(event(name = "test_event", namespace = "production"));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.namespace, Some("production".to_string()));

        let item: ItemFn = parse_quote! {
            async fn my_event() -> Result<()> {
                Ok(())
            }
        };

        let result = event_impl_inner(attrs, item);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_impl_with_max_queue_size() {
        let meta: Meta = parse_quote!(event(name = "test_event", max_queue_size = 500));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.max_queue_size, Some(500));
    }

    #[test]
    fn test_event_impl_default_queue_size() {
        let attrs = EventAttrs::default();
        assert_eq!(attrs.max_queue_size, Some(1000));
    }

    #[test]
    fn test_event_impl_with_version() {
        let meta: Meta = parse_quote!(event(name = "test_event", version = "2.0.0"));
        let attrs = EventAttrs::from_meta(&meta).unwrap();
        assert_eq!(attrs.version, Some("2.0.0".to_string()));
    }

    #[test]
    fn test_capitalize_first() {
        assert_eq!(capitalize_first("hello"), "Hello");
        assert_eq!(capitalize_first("world"), "World");
        assert_eq!(capitalize_first(""), "");
        assert_eq!(capitalize_first("a"), "A");
    }
}
