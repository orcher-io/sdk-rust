//! Implementation of `#[derive(Payload)]`.
//!
//! For a type `T` that implements serde's `Serialize` and `Deserialize`, the derive adds
//! these inherent methods, all using JSON encoding:
//!
//! - `to_payload(&self) -> Result<Payload, Error>`
//! - `from_payload(payload: &Payload) -> Result<Self, Error>`
//! - `try_into_payload(self) -> Result<Payload, Error>`
//!
//! The macro is documented, with examples, on `#[derive(Payload)]` in the crate root.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, Data, DeriveInput, GenericParam, Generics};

/// Generates the payload methods for a struct or enum.
///
/// # Errors
///
/// Returns an error if the input is a union.
pub fn derive_payload(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;
    let generics = input.generics;

    match &input.data {
        Data::Struct(_) | Data::Enum(_) => {}
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                name,
                "#[derive(Payload)] cannot be used on unions",
            ));
        }
    }

    // Every type parameter must be serializable for the generated methods to compile.
    let generics = add_trait_bounds(generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            /// Serializes this value to a JSON payload.
            ///
            /// # Errors
            ///
            /// Returns an error if serialization fails.
            pub fn to_payload(&self) -> ::std::result::Result<
                ::orcher::Payload,
                ::orcher::Error
            > {
                ::orcher::Payload::from_json(self)
                    .map_err(|e| ::orcher::Error::Serialization(e.to_string()))
            }

            /// Deserializes a value of this type from a JSON payload.
            ///
            /// # Errors
            ///
            /// Returns an error if deserialization fails.
            pub fn from_payload(
                payload: &::orcher::Payload
            ) -> ::std::result::Result<Self, ::orcher::Error> {
                payload.to_json::<Self>()
                    .map_err(|e| ::orcher::Error::Serialization(e.to_string()))
            }

            /// Consumes this value and serializes it to a JSON payload.
            ///
            /// # Errors
            ///
            /// Returns an error if serialization fails.
            pub fn try_into_payload(
                self
            ) -> ::std::result::Result<
                ::orcher::Payload,
                ::orcher::Error
            > {
                self.to_payload()
            }
        }

        // No `PayloadCodec` impl is generated: the SDK provides a blanket impl for every
        // `Serialize + Deserialize` type, and a second impl would conflict with it.
    };

    Ok(expanded)
}

/// Adds `Serialize` and `DeserializeOwned` bounds to every type parameter.
fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(::serde::Serialize));
            type_param
                .bounds
                .push(parse_quote!(::serde::de::DeserializeOwned));
        }
    }
    generics
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_derive_payload_struct() {
        let input: DeriveInput = parse_quote! {
            struct MyData {
                field1: String,
                field2: i32,
            }
        };

        let result = derive_payload(input);
        assert!(result.is_ok());

        let tokens = result.unwrap();
        let code = tokens.to_string();

        assert!(code.contains("fn to_payload"));
        assert!(code.contains("fn from_payload"));
        assert!(code.contains("fn try_into_payload"));
        // `PayloadCodec` comes from the SDK's blanket impl, not from this derive.
    }

    #[test]
    fn test_derive_payload_enum() {
        let input: DeriveInput = parse_quote! {
            enum MyEnum {
                VariantA,
                VariantB(String),
                VariantC { field: i32 },
            }
        };

        let result = derive_payload(input);
        assert!(result.is_ok());
    }

    #[test]
    fn test_derive_payload_with_generics() {
        let input: DeriveInput = parse_quote! {
            struct GenericData<T> {
                value: T,
            }
        };

        let result = derive_payload(input);
        assert!(result.is_ok());

        let tokens = result.unwrap();
        let code = tokens.to_string();

        // Type parameters get serde bounds.
        assert!(code.contains("Serialize"));
        assert!(code.contains("DeserializeOwned"));
        assert!(code.contains("fn to_payload"));
        assert!(code.contains("fn from_payload"));
    }

    #[test]
    fn test_derive_payload_union_fails() {
        let input: DeriveInput = parse_quote! {
            union MyUnion {
                field1: i32,
                field2: f64,
            }
        };

        let result = derive_payload(input);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot be used on unions"));
    }

    #[test]
    fn test_derive_payload_with_lifetime() {
        let input: DeriveInput = parse_quote! {
            struct MyData<'a> {
                field: &'a str,
            }
        };

        let result = derive_payload(input);
        assert!(result.is_ok());
    }

    #[test]
    fn test_derive_payload_complex_generics() {
        let input: DeriveInput = parse_quote! {
            struct ComplexData<T, U>
            where
                T: Clone,
                U: Default,
            {
                field1: T,
                field2: U,
            }
        };

        let result = derive_payload(input);
        assert!(result.is_ok());

        let tokens = result.unwrap();
        let code = tokens.to_string();

        // The existing where clause is kept alongside the added bounds.
        assert!(code.contains("Clone"));
        assert!(code.contains("Default"));
        assert!(code.contains("Serialize"));
    }
}
