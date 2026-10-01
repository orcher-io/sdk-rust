//! Typed references to workflow update handlers.

/// A typed reference to an update handler.
///
/// The `#[update]` macro implements this on a zero-sized struct, so callers name a handler
/// by a Rust item instead of a string. A typo or a renamed handler fails at compile time
/// rather than at run time.
///
/// # Example
///
/// ```rust
/// use orcher::prelude::*;
/// use orcher::workflow::update::UpdateReference;
///
/// #[derive(Serialize, Deserialize)]
/// struct Address {
///     street: String,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct AddressChange {
///     accepted: bool,
/// }
///
/// #[update]
/// async fn change_address(ctx: WorkflowContext, address: Address) -> Result<AddressChange> {
///     ctx.set_state("address", &address)?;
///     Ok(AddressChange { accepted: true })
/// }
///
/// // The macro generates a reference with the handler's name:
/// assert_eq!(change_address.update_name(), "change_address");
///
/// // Client usage:
/// async fn move_order(handle: &WorkflowHandle) -> Result<AddressChange> {
///     let address = Address { street: "1 Main St".to_string() };
///     handle.update(change_address.update_name(), address).await
/// }
/// ```
pub trait UpdateReference: Send + Sync + 'static {
    /// Returns the handler name as registered with the worker.
    fn update_name(&self) -> &'static str;
}
