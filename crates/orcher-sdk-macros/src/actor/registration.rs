//! Registration code for actor operation handlers (`auto-register` feature).
//!
//! For each `#[operation]` method, `#[operations]` emits a `#[ctor::ctor]` function that
//! adds the handler to `GLOBAL_REGISTRY` at program startup. The handler deserializes the
//! operation's arguments from a JSON tuple, calls the renamed `<name>_impl` method, and
//! serializes the result as JSON.
//! # Example
//!
//! ```text
//! #[operations]
//! impl ShoppingCart {
//!     #[operation(exclusive)]
//!     async fn add_item(ctx: ActorContext, item: Item) -> Result<Cart> {
//!         // ...
//!     }
//! }
//! ```
//!
//! Expands to roughly:
//!
//! ```text
//! impl ShoppingCart {
//!     async fn add_item_impl(ctx: ActorContext, item: Item) -> Result<Cart> {
//!         // original body
//!     }
//! }
//!
//! #[::orcher_sdk::ctor::ctor]
//! fn __orcher_register_actor_handler_ShoppingCart_add_item() {
//!     ::orcher_sdk::worker::registry::GLOBAL_REGISTRY
//!         .write()
//!         .unwrap()
//!         .register_actor_handler_direct(
//!             ::orcher_sdk::worker::registration::ActorHandlerRegistration {
//!                 actor_name: "ShoppingCart",
//!                 operation_name: "add_item",
//!                 mode: ::orcher_sdk::actor::OperationMode::Exclusive,
//!                 handler: ::std::sync::Arc::new(|ctx, payload| {
//!                     ::std::boxed::Box::pin(async move {
//!                         // Arguments arrive as a JSON tuple.
//!                         let (item,): (Item,) = ::serde_json::from_slice(&payload)
//!                             .map_err(|e| ::orcher_sdk::error::Error::Worker(
//!                                 ::orcher_sdk::error::WorkerError::InvalidConfiguration(
//!                                     format!("Failed to deserialize parameters: {}", e)
//!                                 )
//!                             ))?;
//!
//!                         let result = ShoppingCart::add_item_impl(ctx, item).await?;
//!
//!                         ::serde_json::to_vec(&result)
//!                             .map_err(|e| ::orcher_sdk::error::Error::Worker(
//!                                 ::orcher_sdk::error::WorkerError::InvalidConfiguration(
//!                                     format!("Failed to serialize result: {}", e)
//!                                 )
//!                             ))
//!                     })
//!                 }),
//!             }
//!         );
//! }
//! ```

#[cfg(feature = "auto-register")]
use crate::actor::codegen::OperationInfo;
#[cfg(feature = "auto-register")]
use proc_macro2::TokenStream;
#[cfg(feature = "auto-register")]
use quote::{format_ident, quote};
#[cfg(feature = "auto-register")]
use syn::Ident;

/// Generates the `#[ctor::ctor]` function that registers one operation's handler in
/// `GLOBAL_REGISTRY` at program startup.
#[cfg(feature = "auto-register")]
pub fn generate_handler_registration(
    struct_name: &Ident,
    _actor_name: &str,
    operation: &OperationInfo,
) -> TokenStream {
    let operation_name = &operation.name;
    let operation_name_str = operation_name.to_string();
    let method_name_impl = format_ident!("{}_impl", operation_name);

    let ctor_fn_name = format_ident!(
        "__orcher_register_actor_handler_{}_{}",
        struct_name,
        operation_name
    );

    let operation_mode = match operation.mode {
        crate::actor::attrs::OperationModeAttr::Exclusive => {
            quote! { ::orcher_sdk::actor::types::OperationMode::Exclusive }
        }
        crate::actor::attrs::OperationModeAttr::Shared => {
            quote! { ::orcher_sdk::actor::types::OperationMode::Shared }
        }
    };

    let param_types: Vec<_> = operation.inputs.iter().map(|(_, ty)| ty).collect();

    // Arguments always arrive as a JSON tuple, matching what the typed client sends.
    let (deserialize_params, param_values) = if param_types.is_empty() {
        (quote! {}, quote! {})
    } else if param_types.len() == 1 {
        let param_ty = &param_types[0];
        (
            quote! {
                let (param,): (#param_ty,) = ::orcher_sdk::__private::serde_json::from_slice(&payload)
                    .map_err(|e| ::orcher_sdk::error::Error::Worker(
                        ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                            format!("Failed to deserialize parameters: {}", e)
                        )
                    ))?;
            },
            quote! { param },
        )
    } else {
        let indices: Vec<_> = (0..param_types.len()).map(syn::Index::from).collect();
        (
            quote! {
                let params: (#(#param_types,)*) = ::orcher_sdk::__private::serde_json::from_slice(&payload)
                    .map_err(|e| ::orcher_sdk::error::Error::Worker(
                        ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                            format!("Failed to deserialize parameters: {}", e)
                        )
                    ))?;
            },
            quote! { #(params.#indices),* },
        )
    };

    quote! {
        #[allow(non_snake_case)]
        #[::orcher_sdk::ctor::ctor]
        fn #ctor_fn_name() {
            ::orcher_sdk::worker::registry::GLOBAL_REGISTRY
                .write()
                .unwrap()
                .register_actor_handler_direct(
                    ::orcher_sdk::worker::registration::ActorHandlerRegistration {
                        // `ACTOR_NAME` comes from `#[actor]`, so a custom
                        // `#[actor(name = "...")]` is honored. This macro cannot see the
                        // `#[actor]` arguments, but it can reference the constant.
                        actor_name: #struct_name::ACTOR_NAME,
                        operation_name: #operation_name_str,
                        mode: #operation_mode,
                        handler: ::std::sync::Arc::new(|ctx, payload| {
                            ::std::boxed::Box::pin(async move {
                                #deserialize_params

                                // Shared operations take a `SharedActorContext`; `Into` converts to either.
                                let result = #struct_name::#method_name_impl(::core::convert::Into::into(ctx), #param_values).await?;

                                ::orcher_sdk::__private::serde_json::to_vec(&result)
                                    .map_err(|e| ::orcher_sdk::error::Error::Worker(
                                        ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                                            format!("Failed to serialize result: {}", e)
                                        )
                                    ))
                            })
                        }),
                    }
                );
        }
    }
}

/// Generates the registration functions for every operation of an actor.
#[cfg(feature = "auto-register")]
pub fn generate_actor_registrations(
    struct_name: &Ident,
    actor_name: &str,
    operations: &[OperationInfo],
) -> TokenStream {
    let registrations: Vec<_> = operations
        .iter()
        .map(|op| generate_handler_registration(struct_name, actor_name, op))
        .collect();

    quote! {
        #(#registrations)*
    }
}

/// Generates a `register_handlers()` method for registering handlers with an executor by
/// hand instead of through `GLOBAL_REGISTRY`.
///
/// It registers under `actor_name`, which the caller sets to the struct name.
///
/// # Example Generated Code
///
/// ```text
/// impl ShoppingCart {
///     pub fn register_handlers(executor: &::orcher_sdk::actor::ActorExecutor) {
///         executor.register_handler(
///             "ShoppingCart",
///             "add_item",
///             ::std::sync::Arc::new(|ctx, payload| {
///                 ::std::boxed::Box::pin(async move {
///                     // handler body
///                 })
///             })
///         );
///         // one call per operation
///     }
/// }
/// ```
#[cfg(feature = "auto-register")]
pub fn generate_register_handlers_method(
    struct_name: &Ident,
    actor_name: &str,
    operations: &[OperationInfo],
) -> TokenStream {
    let handler_registrations: Vec<_> = operations
        .iter()
        .map(|op| {
            let operation_name = &op.name;
            let operation_name_str = operation_name.to_string();
            let method_name_impl = format_ident!("{}_impl", operation_name);

            let param_types: Vec<_> = op.inputs.iter().map(|(_, ty)| ty).collect();

            let (deserialize_params, param_values) = if param_types.is_empty() {
                (quote! {}, quote! {})
            } else if param_types.len() == 1 {
                let param_ty = &param_types[0];
                (
                    quote! {
                        let (param,): (#param_ty,) = ::orcher_sdk::__private::serde_json::from_slice(&payload)
                            .map_err(|e| ::orcher_sdk::error::Error::Worker(
                                ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                                    format!("Failed to deserialize parameters: {}", e)
                                )
                            ))?;
                    },
                    quote! { param },
                )
            } else {
                let indices: Vec<_> = (0..param_types.len()).map(syn::Index::from).collect();
                (
                    quote! {
                        let params: (#(#param_types,)*) = ::orcher_sdk::__private::serde_json::from_slice(&payload)
                            .map_err(|e| ::orcher_sdk::error::Error::Worker(
                                ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                                    format!("Failed to deserialize parameters: {}", e)
                                )
                            ))?;
                    },
                    quote! { #(params.#indices),* },
                )
            };

            quote! {
                executor.register_handler(
                    #actor_name,
                    #operation_name_str,
                    ::std::sync::Arc::new(|ctx, payload| {
                        ::std::boxed::Box::pin(async move {
                            #deserialize_params

                            // Shared operations take a `SharedActorContext`; `Into` converts to either.
                            let result = #struct_name::#method_name_impl(::core::convert::Into::into(ctx), #param_values).await?;

                            ::orcher_sdk::__private::serde_json::to_vec(&result)
                                .map_err(|e| ::orcher_sdk::error::Error::Worker(
                                    ::orcher_sdk::error::WorkerError::InvalidConfiguration(
                                        format!("Failed to serialize result: {}", e)
                                    )
                                ))
                        })
                    })
                );
            }
        })
        .collect();

    quote! {
        impl #struct_name {
            /// Registers every operation handler of this actor with `executor`.
            ///
            /// Use this instead of automatic registration when you need to control which
            /// executor the handlers go to.
            ///
            /// # Example
            ///
            /// ```text
            /// let executor = ActorExecutor::new();
            /// ShoppingCart::register_handlers(&executor);
            /// ```
            pub fn register_handlers(executor: &::orcher_sdk::actor::ActorExecutor) {
                #(#handler_registrations)*
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_handler_registration_no_params() {
        let struct_name = format_ident!("Counter");
        let actor_name = "Counter";

        let operation = OperationInfo {
            name: format_ident!("get_count"),
            mode: crate::actor::attrs::OperationModeAttr::Shared,
            inputs: vec![],
            output: syn::parse_quote! { -> Result<i64> },
            is_async: true,
            vis: syn::parse_quote! { pub },
        };

        let result = generate_handler_registration(&struct_name, actor_name, &operation);
        let output = result.to_string();

        // Token strings contain spaces, so match on fragments.
        assert!(output.contains("__orcher_register_actor_handler_Counter_get_count"));
        assert!(output.contains("orcher_sdk") && output.contains(":: ctor"));
        assert!(output.contains("GLOBAL_REGISTRY"));
        assert!(output.contains("register_actor_handler_direct"));
        assert!(output.contains("Counter"));
        assert!(output.contains("get_count"));
    }

    #[test]
    fn test_generate_handler_registration_with_params() {
        let struct_name = format_ident!("Counter");
        let actor_name = "Counter";

        let operation = OperationInfo {
            name: format_ident!("increment"),
            mode: crate::actor::attrs::OperationModeAttr::Exclusive,
            inputs: vec![(format_ident!("delta"), syn::parse_quote! { i64 })],
            output: syn::parse_quote! { -> Result<i64> },
            is_async: true,
            vis: syn::parse_quote! { pub },
        };

        let result = generate_handler_registration(&struct_name, actor_name, &operation);
        let output = result.to_string();

        // Token strings contain spaces, so match on fragments.
        assert!(output.contains("serde_json") && output.contains("from_slice"));
        assert!(output.contains("increment_impl"));
    }

    #[test]
    fn test_generate_actor_registrations_multiple() {
        let struct_name = format_ident!("Counter");
        let actor_name = "Counter";

        let operations = vec![
            OperationInfo {
                name: format_ident!("increment"),
                mode: crate::actor::attrs::OperationModeAttr::Exclusive,
                inputs: vec![(format_ident!("delta"), syn::parse_quote! { i64 })],
                output: syn::parse_quote! { -> Result<i64> },
                is_async: true,
                vis: syn::parse_quote! { pub },
            },
            OperationInfo {
                name: format_ident!("get_count"),
                mode: crate::actor::attrs::OperationModeAttr::Shared,
                inputs: vec![],
                output: syn::parse_quote! { -> Result<i64> },
                is_async: true,
                vis: syn::parse_quote! { pub },
            },
        ];

        let result = generate_actor_registrations(&struct_name, actor_name, &operations);
        let output = result.to_string();

        assert!(output.contains("__orcher_register_actor_handler_Counter_increment"));
        assert!(output.contains("__orcher_register_actor_handler_Counter_get_count"));
    }

    #[test]
    fn test_generate_register_handlers_method() {
        let struct_name = format_ident!("Counter");
        let actor_name = "Counter";

        let operations = vec![OperationInfo {
            name: format_ident!("increment"),
            mode: crate::actor::attrs::OperationModeAttr::Exclusive,
            inputs: vec![(format_ident!("delta"), syn::parse_quote! { i64 })],
            output: syn::parse_quote! { -> Result<i64> },
            is_async: true,
            vis: syn::parse_quote! { pub },
        }];

        let result = generate_register_handlers_method(&struct_name, actor_name, &operations);
        let output = result.to_string();

        // Token strings contain spaces, so match on fragments.
        assert!(output.contains("pub fn register_handlers"));
        assert!(output.contains("ActorExecutor"));
        assert!(output.contains("executor") && output.contains("register_handler"));
        assert!(output.contains("increment"));
    }
}
