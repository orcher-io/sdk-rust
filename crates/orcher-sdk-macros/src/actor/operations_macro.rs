//! Implementation of the `#[operations]` attribute macro, which turns the `#[operation]`
//! methods of an actor's impl block into invocable operations with a typed client.

use crate::actor::codegen::{
    extract_operations, generate_operations_metadata, generate_typed_client, rename_impl_method,
};

#[cfg(feature = "auto-register")]
use crate::actor::registration::{generate_actor_registrations, generate_register_handlers_method};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ItemImpl};

/// Entry point for `#[operations]`.
///
/// Each `#[operation]` method is renamed to `<name>_impl`; other methods are kept as
/// written. The macro also generates the `operations()` metadata method, a typed
/// `<Actor>Client`, and, with the `auto-register` feature, handler registration.
///
/// # Errors
///
/// Emits a compile error if the block is not on a named type, contains no `#[operation]`
/// method, or an operation's signature is invalid.
///
/// # Examples
///
/// ```text
/// #[operations]
/// impl ShoppingCart {
///     #[operation(exclusive)]
///     pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Cart> {
///         let mut cart = ctx.state().get::<Cart>("cart").await?.unwrap_or_default();
///         cart.items.push(item);
///         ctx.state().set("cart", &cart).await?;
///         Ok(cart)
///     }
///
///     #[operation(shared)]
///     pub async fn view_cart(ctx: SharedActorContext) -> Result<Cart> {
///         Ok(ctx.state().get::<Cart>("cart").await?.unwrap_or_default())
///     }
/// }
/// ```
///
/// This generates:
/// - `ShoppingCart::operations()`, listing each operation's name and mode
/// - `add_item_impl` and `view_cart_impl`, the renamed methods
/// - `ShoppingCartClient`, a typed client
/// - with `auto-register`: a `#[ctor::ctor]` registration function per operation and
///   `ShoppingCart::register_handlers()` for manual registration
pub fn operations_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let impl_block = parse_macro_input!(item as ItemImpl);

    let attrs = if attr.is_empty() {
        crate::actor::attrs::OperationsAttrs::default()
    } else {
        // `#[operations]` defines no arguments, so any given are ignored.
        crate::actor::attrs::OperationsAttrs::default()
    };

    match operations_impl_inner(impl_block, attrs) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn operations_impl_inner(
    impl_block: ItemImpl,
    _attrs: crate::actor::attrs::OperationsAttrs,
) -> Result<TokenStream2, syn::Error> {
    let struct_name = if let syn::Type::Path(type_path) = &*impl_block.self_ty {
        type_path
            .path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new_spanned(&impl_block.self_ty, "Invalid type path"))?
            .ident
            .clone()
    } else {
        return Err(syn::Error::new_spanned(
            &impl_block.self_ty,
            "Expected a named struct type",
        ));
    };

    let operations = extract_operations(&impl_block)?;

    if operations.is_empty() {
        return Err(syn::Error::new_spanned(
            &impl_block,
            "No operations found. Mark methods with #[operation(exclusive)] or #[operation(shared)]",
        ));
    }

    // The manual `register_handlers()` path registers under the struct name. The automatic
    // path reads `ACTOR_NAME` instead, so it honors a custom `#[actor(name = ...)]`.
    #[cfg(feature = "auto-register")]
    let actor_name = struct_name.to_string();

    let metadata_impl = generate_operations_metadata(&struct_name, &operations);

    let typed_client = generate_typed_client(&struct_name, &operations);

    // Only methods are re-emitted; other items in the block are not.
    let impl_methods: Vec<_> = impl_block
        .items
        .iter()
        .filter_map(|item| {
            if let syn::ImplItem::Fn(method) = item {
                let has_operation_attr = method
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("operation"));

                if has_operation_attr {
                    Some(rename_impl_method(method))
                } else {
                    Some(method.clone())
                }
            } else {
                None
            }
        })
        .collect();

    #[cfg(feature = "auto-register")]
    let handler_registrations =
        generate_actor_registrations(&struct_name, &actor_name, &operations);

    #[cfg(not(feature = "auto-register"))]
    let handler_registrations = quote! {};

    #[cfg(feature = "auto-register")]
    let manual_registration =
        generate_register_handlers_method(&struct_name, &actor_name, &operations);

    #[cfg(not(feature = "auto-register"))]
    let manual_registration = quote! {};

    Ok(quote! {
        impl #struct_name {
            #(#impl_methods)*
        }

        #metadata_impl

        #typed_client

        #manual_registration

        #handler_registrations
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn test_operations_macro_basic() {
        let input = quote! {
            impl ShoppingCart {
                #[operation(exclusive)]
                pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Cart> {
                    todo!()
                }
            }
        };

        let attrs = crate::actor::attrs::OperationsAttrs::default();
        let result = operations_impl_inner(syn::parse2(input).unwrap(), attrs);
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("add_item_impl"));
        assert!(output.contains("operations"));
    }

    #[test]
    fn test_operations_macro_no_operations() {
        let input = quote! {
            impl ShoppingCart {
                pub async fn helper_method(&self) -> Result<()> {
                    Ok(())
                }
            }
        };

        let attrs = crate::actor::attrs::OperationsAttrs::default();
        let result = operations_impl_inner(syn::parse2(input).unwrap(), attrs);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No operations found"));
    }

    #[test]
    fn test_operations_macro_mixed_operations() {
        let input = quote! {
            impl ShoppingCart {
                #[operation(exclusive)]
                pub async fn add_item(ctx: ActorContext, item: Item) -> Result<Cart> {
                    todo!()
                }

                #[operation(shared)]
                pub async fn view_cart(ctx: SharedActorContext) -> Result<Cart> {
                    todo!()
                }

                // Helper method without #[operation]
                async fn helper(&self) -> Result<()> {
                    Ok(())
                }
            }
        };

        let attrs = crate::actor::attrs::OperationsAttrs::default();
        let result = operations_impl_inner(syn::parse2(input).unwrap(), attrs);
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("add_item_impl"));
        assert!(output.contains("view_cart_impl"));
        assert!(output.contains("helper")); // Non-operation methods are kept.
    }
}
