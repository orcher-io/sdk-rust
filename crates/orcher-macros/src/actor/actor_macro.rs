//! Implementation of the `#[actor]` attribute macro, which marks a struct as a stateful
//! actor type.

use crate::actor::attrs::{validate_actor_name, ActorAttrs};
use crate::actor::codegen::{
    determine_actor_name, extract_struct_name, generate_actor_name_constant,
    generate_actor_trait_impl,
};
use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, ItemStruct, Meta};

/// Entry point for `#[actor]`.
///
/// Keeps the struct as written and adds an `Actor` trait implementation and an `ACTOR_NAME`
/// constant. The actor name defaults to the struct name.
///
/// # Errors
///
/// Emits a compile error if the arguments do not parse or the actor name is invalid (see
/// [`validate_actor_name`]).
/// # Examples
///
/// ```text
/// // The actor name is the struct name.
/// #[actor]
/// pub struct ShoppingCart;
///
/// // An explicit actor name.
/// #[actor(name = "CustomShoppingCart")]
/// pub struct ShoppingCart;
/// ```
pub fn actor_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(item as ItemStruct);

    let attrs = if attr.is_empty() {
        ActorAttrs::default()
    } else {
        let attr2: proc_macro2::TokenStream = attr.clone().into();

        // Wrap the arguments as `actor(...)` so darling can parse them as a list.
        let wrapped_tokens = quote::quote! { actor(#attr2) };

        match syn::parse2::<Meta>(wrapped_tokens) {
            Ok(meta) => match ActorAttrs::from_meta(&meta) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return syn::Error::new(
                        proc_macro2::Span::call_site(),
                        format!("Failed to parse actor attributes: {}", e),
                    )
                    .to_compile_error()
                    .into();
                }
            },
            Err(e) => return e.to_compile_error().into(),
        }
    };

    match actor_impl_inner(attrs, item_struct) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn actor_impl_inner(
    attrs: ActorAttrs,
    item_struct: ItemStruct,
) -> Result<TokenStream2, syn::Error> {
    let struct_name = extract_struct_name(&item_struct);
    let actor_name = determine_actor_name(&attrs, &struct_name);

    validate_actor_name(&actor_name).map_err(|e| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid actor name '{}': {}", actor_name, e),
        )
    })?;

    let trait_impl = generate_actor_trait_impl(&struct_name, &actor_name);

    let name_constant = generate_actor_name_constant(&struct_name, &actor_name);

    let vis = &item_struct.vis;
    let attrs_tokens = &item_struct.attrs;
    let generics = &item_struct.generics;
    let fields = &item_struct.fields;

    // A unit struct needs its trailing semicolon back.
    let semi = if matches!(fields, syn::Fields::Unit) {
        quote! { ; }
    } else {
        quote! {}
    };

    Ok(quote! {
        #(#attrs_tokens)*
        #vis struct #struct_name #generics #fields #semi

        #trait_impl

        #name_constant
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn test_actor_macro_empty_attrs() {
        let input = quote! {
            pub struct ShoppingCart;
        };

        let result = actor_impl_inner(ActorAttrs::default(), syn::parse2(input).unwrap());
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("impl"));
        assert!(output.contains("Actor"));
        assert!(output.contains("ShoppingCart"));
        assert!(output.contains("ACTOR_NAME"));
    }

    #[test]
    fn test_actor_macro_with_custom_name() {
        let input = quote! {
            pub struct ShoppingCart;
        };

        let attrs = ActorAttrs {
            name: Some("CustomCart".to_string()),
        };

        let result = actor_impl_inner(attrs, syn::parse2(input).unwrap());
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("CustomCart"));
    }

    #[test]
    fn test_actor_macro_invalid_name() {
        let input = quote! {
            pub struct ShoppingCart;
        };

        let attrs = ActorAttrs {
            name: Some("123Invalid".to_string()),
        };

        let result = actor_impl_inner(attrs, syn::parse2(input).unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_actor_macro_with_fields() {
        let input = quote! {
            pub struct ShoppingCart {
                id: String,
            }
        };

        let result = actor_impl_inner(ActorAttrs::default(), syn::parse2(input).unwrap());
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("id"));
        assert!(output.contains("String"));
    }

    #[test]
    fn test_actor_macro_preserves_attributes() {
        let input = quote! {
            #[derive(Debug, Clone)]
            pub struct ShoppingCart;
        };

        let result = actor_impl_inner(ActorAttrs::default(), syn::parse2(input).unwrap());
        assert!(result.is_ok());

        let output = result.unwrap().to_string();
        assert!(output.contains("derive"));
        assert!(output.contains("Debug"));
        assert!(output.contains("Clone"));
    }
}
