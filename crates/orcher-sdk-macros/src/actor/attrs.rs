//! Attribute parsing and validation for the actor macros.

use darling::FromMeta;

/// Arguments accepted by `#[actor]`.
///
/// # Examples
///
/// ```text
/// #[actor]
/// pub struct ShoppingCart;
///
/// #[actor(name = "CustomName")]
/// pub struct BankAccount;
/// ```
#[derive(Debug, Clone, FromMeta, Default)]
pub struct ActorAttrs {
    /// Actor name; defaults to the struct name.
    #[darling(default)]
    pub name: Option<String>,
}

/// Arguments accepted by `#[operations]`. None are defined; the type exists so that
/// arguments can be added without changing the parser.
///
/// # Examples
///
/// ```text
/// #[operations]
/// impl ShoppingCart {
///     // operations here
/// }
/// ```
#[derive(Debug, Clone, FromMeta, Default)]
pub struct OperationsAttrs {
    // No arguments are defined.
}

/// Concurrency mode of an actor operation, set with `#[operation(exclusive)]` or
/// `#[operation(shared)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OperationModeAttr {
    /// Runs alone: exclusive operations on the same actor key are serialized.
    #[default]
    Exclusive,
    /// May run concurrently with other shared operations; intended for reads.
    Shared,
}

/// Parses the mode from an `#[operation(exclusive)]` or `#[operation(shared)]` attribute.
///
/// Returns `Ok(None)` for attributes other than `operation`.
///
/// # Errors
///
/// Returns an error if the argument is not `exclusive` or `shared`.
pub fn parse_operation_mode(attr: &syn::Attribute) -> syn::Result<Option<OperationModeAttr>> {
    if !attr.path().is_ident("operation") {
        return Ok(None);
    }

    let mode = attr.parse_args_with(|input: syn::parse::ParseStream| {
        let ident: syn::Ident = input.parse()?;
        match ident.to_string().as_str() {
            "exclusive" => Ok(OperationModeAttr::Exclusive),
            "shared" => Ok(OperationModeAttr::Shared),
            other => Err(syn::Error::new(
                ident.span(),
                format!(
                    "Invalid operation mode: '{}'. Expected 'exclusive' or 'shared'",
                    other
                ),
            )),
        }
    })?;

    Ok(Some(mode))
}

/// Checks that an actor name is non-empty, starts with a letter, and contains only
/// alphanumeric characters, underscores and hyphens.
pub fn validate_actor_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Actor name cannot be empty".to_string());
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_alphabetic() {
        return Err(format!("Actor name '{}' must start with a letter", name));
    }

    for ch in name.chars() {
        if !ch.is_alphanumeric() && ch != '_' && ch != '-' {
            return Err(format!(
                "Actor name '{}' contains invalid character '{}'. Only alphanumeric, underscore, and hyphen are allowed",
                name, ch
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_actor_name_valid() {
        assert!(validate_actor_name("ShoppingCart").is_ok());
        assert!(validate_actor_name("shopping_cart").is_ok());
        assert!(validate_actor_name("shopping-cart").is_ok());
        assert!(validate_actor_name("ShoppingCart123").is_ok());
        assert!(validate_actor_name("A").is_ok());
    }

    #[test]
    fn test_validate_actor_name_invalid() {
        assert!(validate_actor_name("").is_err());
        assert!(validate_actor_name("123Cart").is_err());
        assert!(validate_actor_name("shopping.cart").is_err());
        assert!(validate_actor_name("shopping cart").is_err());
        assert!(validate_actor_name("shopping@cart").is_err());
    }

    #[test]
    fn test_operation_mode_default() {
        assert_eq!(OperationModeAttr::default(), OperationModeAttr::Exclusive);
    }
}
