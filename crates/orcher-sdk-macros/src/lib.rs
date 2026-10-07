//! # ORCHER Macros
//!
//! Procedural macros for defining Orcher workflows, tasks, handlers, and actors.
//!
//! Each macro keeps the item you wrote and generates the glue around it: payload
//! (de)serialization, typed references, and registration so a worker finds the item at
//! startup without manual wiring.
//!
//! ## Available Macros
//!
//! - `#[task]` and `#[tasks]`: define tasks, the units of work that touch the outside world.
//! - `#[workflow]`: define a workflow, the deterministic code that orchestrates tasks.
//! - `#[query]`, `#[update]`, `#[event]`: handlers that read, change, or send events to a
//!   running workflow.
//! - `#[actor]`, `#[operations]`, `#[operation]`: stateful actors.
//! - `#[derive(Payload)]` (feature `derive`): payload conversion methods for a type.
//!
//! These macros are re-exported by the `orcher-sdk` crate, which the generated code refers
//! to, so depend on `orcher-sdk` and import them from its prelude.
//!
//! ## Examples
//!
//! ### Task Macro
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[task(
//!     retry_policy(max_attempts = 3, initial_interval = 1, backoff_coefficient = 2.0),
//!     timeout = 30,
//!     resources(cpu = 1.0, memory = "512Mi")
//! )]
//! async fn process_data(_ctx: TaskContext, input: String) -> Result<String> {
//!     // Do the work here.
//!     Ok(format!("Processed: {input}"))
//! }
//! ```
//!
//! ### Workflow Macro
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! # #[task]
//! # async fn process_data(_ctx: TaskContext, input: String) -> Result<String> { Ok(input) }
//! #[workflow(name = "data-pipeline")]
//! async fn data_pipeline(ctx: WorkflowContext, input: String) -> Result<String> {
//!     // Orchestrate tasks here.
//!     ctx.execute_task(process_data, input).await
//! }
//! ```

extern crate proc_macro;

mod actor;
mod common;
mod event;
mod query;
mod task;
mod update;
mod workflow;

#[cfg(feature = "derive")]
mod derive;

use proc_macro::TokenStream;

/// Turns an `async fn` into a task that workers can run.
///
/// The function must be `async`, take `TaskContext` as its first parameter, and have a
/// return type. The macro keeps the function and adds a payload-level handler, a typed
/// task reference, and a registration that makes the task available to workers.
///
/// # Attributes
///
/// - `name`: task name; defaults to the function name. Workflows schedule tasks by
///   this name.
/// - `description`, `version`, `namespace`: descriptive metadata.
/// - `timeout`: timeout in seconds. `timeout_mins` sets it in minutes instead.
/// - `heartbeat_timeout`: seconds without a heartbeat before the task counts as stalled.
/// - `retry`: maximum attempts, with exponential backoff (1 s initial, 60 s maximum,
///   coefficient 2.0).
/// - `retry_policy(...)`: an explicit policy; takes precedence over `retry`.
///   - `max_attempts`: maximum attempts, including the first (default: 3)
///   - `initial_interval`: first retry delay in seconds (default: 1)
///   - `max_interval`: cap on the retry delay in seconds (default: 60)
///   - `backoff_coefficient`: multiplier applied to the delay after each retry (default: 2.0)
/// - `non_retryable_errors = ["..."]`: error types that are never retried.
/// - `resources(cpu, memory, disk)`: resource requirements, for example `cpu = 0.5`,
///   `memory = "512Mi"`, `disk = "1Gi"`. `memory = "..."` is a shortcut for the memory
///   request.
/// - `preset`: `"long-running"`, `"quick"`, or `"critical"`. Fills in retry, timeout, and
///   memory values that are not set explicitly.
///
/// `task_queue`, `priority`, `max_concurrent`, and `rate_limit` are accepted but not
/// applied by the worker. Unknown attributes are a compile error.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[task(
///     name = "fetch_data",
///     description = "Fetch data from external API",
///     retry_policy(max_attempts = 5, initial_interval = 2, max_interval = 30),
///     non_retryable_errors = ["NotFound"],
///     timeout = 60,
///     heartbeat_timeout = 10,
///     resources(cpu = 0.5, memory = "256Mi")
/// )]
/// async fn fetch_data(ctx: TaskContext, url: String) -> Result<String> {
///     // Call the API here, reporting progress as it goes.
///     ctx.heartbeat().await?;
///     Ok(format!("data from {url}"))
/// }
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::task_impl(attr, item)
}

/// Defines a group of tasks as methods on an impl block.
///
/// Mark each task method with `#[task]`. Attributes on `#[tasks(...)]` are defaults for
/// every task in the group; a method's own `#[task(...)]` attributes override them.
///
/// # Group Attributes
///
/// - `retry`: default maximum attempts
/// - `timeout`: default timeout in seconds
/// - `timeout_mins`: default timeout in minutes
/// - `heartbeat_timeout`: default heartbeat timeout in seconds
/// - `memory`: default memory request
/// - `preset`: default preset (`"long-running"`, `"quick"`, `"critical"`)
/// - `namespace`, `version`: default metadata
/// - `task_queue`, `priority`, `max_concurrent`, `rate_limit`: accepted but not applied,
///   as on `#[task]`
///
/// # Method Attributes
///
/// Each `#[task]` method accepts the same attributes as a standalone `#[task]`.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// pub struct Payment {
///     amount_cents: u64,
/// }
///
/// pub struct PaymentTasks;
///
/// #[tasks(retry = 3, timeout = 60)]
/// impl PaymentTasks {
///     #[task]
///     async fn charge(_ctx: TaskContext, payment: Payment) -> Result<String> {
///         // Uses the group defaults: retry = 3, timeout = 60.
///         Ok(format!("charged {}", payment.amount_cents))
///     }
///
///     #[task(retry = 10)]
///     async fn refund(_ctx: TaskContext, charge_id: String) -> Result<bool> {
///         // Overrides retry; keeps timeout = 60 from the group.
///         Ok(!charge_id.is_empty())
///     }
/// }
///
/// // In a workflow, call a task through its typed reference:
/// #[workflow]
/// async fn checkout(ctx: WorkflowContext, payment: Payment) -> Result<String> {
///     ctx.execute_task(PaymentTasks::charge, payment).await
/// }
/// ```
#[proc_macro_attribute]
pub fn tasks(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::tasks_impl(attr, item)
}

/// Turns an `async fn` into a workflow definition.
///
/// The function must be `async`, take `WorkflowContext` as its first parameter, and have
/// a return type. Workflow code must be deterministic so it can be replayed: use the
/// context for time, randomness, and side effects, and run I/O in tasks.
///
/// # Attributes
///
/// - `name`: workflow name; defaults to the function name.
/// - `description`, `task_queue`: metadata.
/// - `max_concurrent`: maximum concurrent steps (default: 10).
/// - `tags("a", "b")`: tags for categorization.
/// - `retry_policy(...)`: retry policy for the whole workflow, with the same keys as on
///   `#[task]`.
/// - `cron`: a 5- or 6-field cron expression that runs the workflow on a schedule.
///   `schedule` is an alias; `cron` wins if both are set.
/// - `enabled`: whether the workflow is enabled (default: `true`).
///
/// Unrecognized attributes are ignored.
///
/// `timeout` and `version` are accepted but have no effect, and each produces a deprecation
/// warning that names the replacement: set the execution timeout per start with
/// `StartWorkflowOptions::with_workflow_execution_timeout`, and the code release with
/// `WorkerBuilder::version_id`.
///
/// There is no `namespace` option: a workflow runs in whatever namespace its worker serves,
/// so the namespace is set on the worker (`WorkerBuilder::namespace`) and where workflows are
/// started (`ClientConfig::with_namespace`, `StartWorkflowOptions::with_namespace`).
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[workflow(
///     name = "etl-pipeline",
///     description = "Extract, transform, and load data",
///     max_concurrent = 5,
///     tags("etl", "data", "production"),
///     retry_policy(max_attempts = 3, initial_interval = 5)
/// )]
/// async fn etl_pipeline(ctx: WorkflowContext, source: String) -> Result<u64> {
///     // Orchestrate tasks here.
///     let rows: u64 = ctx.execute_task("extract", source).await?;
///     Ok(rows)
/// }
/// ```
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    workflow::workflow_impl(attr, item)
}

/// Declares a query: a read of a running workflow's state that does not affect its
/// execution.
///
/// The macro keeps the function and generates a `<Name>Query` struct (the function name
/// with its first letter capitalized) holding the query's name and settings. It does not
/// register the function: a workflow answers the query by passing a handler to
/// `WorkflowContext::register_query_handler`, as below.
///
/// # Attributes
///
/// - `name`: query name; defaults to the function name.
/// - `description`: query description.
/// - `timeout`: query timeout in seconds.
/// - `namespace`: namespace the query belongs to.
/// - `cache_ttl`: how long a result may be cached, in seconds (0 disables caching).
/// - `version`: query version.
///
/// # Requirements
///
/// - Must not be `async`: queries are meant to be fast reads.
/// - Must have a return type.
/// - Must have no side effects.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[query(
///     name = "order_status",
///     description = "Get current order status",
///     timeout = 10,
///     cache_ttl = 60
/// )]
/// fn status(phase: String) -> String {
///     format!("order is {phase}")
/// }
///
/// // The function is kept, and `StatusQuery` describes the query:
/// assert_eq!(StatusQuery::name(), "order_status");
/// assert_eq!(StatusQuery::cache_ttl_seconds(), Some(60));
///
/// // A running workflow answers the query through a registered handler:
/// #[workflow]
/// async fn order(ctx: WorkflowContext, order_id: String) -> Result<String> {
///     ctx.register_query_handler(StatusQuery::name(), || status("processing".to_string()));
///     ctx.execute_task("ship", order_id).await
/// }
/// ```
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    query::query_impl(attr, item)
}

/// Turns an `async fn` into an update handler for a running workflow.
///
/// An update is a synchronous request, sent with `WorkflowService.UpdateWorkflow`, that
/// runs inside the workflow's execution context. Unlike a query, it can read and change
/// workflow state. Its result is journaled as `UpdateAccepted` and `UpdateCompleted`
/// events, so replay reproduces it.
///
/// # Attributes
///
/// - `name`: update name; defaults to the function name.
/// - `description`: update description.
/// - `timeout`: timeout in seconds.
/// - `namespace`: namespace the update belongs to.
/// - `version`: update version.
///
/// # Requirements
///
/// - Must be `async`.
/// - Must have a return type.
/// - First parameter must be `WorkflowContext`.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct Address {
///     street: String,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct AddressChangeResult {
///     accepted: bool,
/// }
///
/// #[update]
/// async fn change_address(ctx: WorkflowContext, new_address: Address) -> Result<AddressChangeResult> {
///     let phase: Option<String> = ctx.get_state("phase")?;
///     if phase.as_deref() == Some("shipped") {
///         return Ok(AddressChangeResult { accepted: false });
///     }
///     ctx.set_state("address", &new_address)?;
///     Ok(AddressChangeResult { accepted: true })
/// }
///
/// #[update(name = "cancel_order", timeout = 30)]
/// async fn cancel(ctx: WorkflowContext, reason: String) -> Result<bool> {
///     ctx.set_state("phase", &"cancelled")?;
///     ctx.set_state("cancel_reason", &reason)?;
///     Ok(true)
/// }
/// ```
#[proc_macro_attribute]
pub fn update(attr: TokenStream, item: TokenStream) -> TokenStream {
    update::update_impl(attr, item)
}

/// Declares an event: an asynchronous message sent to a running workflow to trigger an
/// action or a state change. The sender does not wait for a result.
///
/// The macro keeps the function and generates a `<Name>Event` struct (the function name
/// with its first letter capitalized) holding the event's name and settings. It does not
/// register the function: a workflow receives the event with
/// `WorkflowContext::wait_for_event`, as below.
///
/// # Attributes
///
/// - `name`: event name; defaults to the function name.
/// - `description`: event description.
/// - `timeout`: handler timeout in seconds.
/// - `namespace`: namespace the event belongs to.
/// - `version`: event version.
/// - `max_queue_size`: maximum number of buffered events (default: 1000).
/// - `priority`: 0 to 100; higher is more important.
///
/// # Requirements
///
/// - Must be `async`.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct Approval {
///     approver: String,
/// }
///
/// #[event(
///     name = "order_approved",
///     description = "Handle order approval",
///     timeout = 30,
///     priority = 90,
///     max_queue_size = 500
/// )]
/// async fn approved(approval: Approval) -> Result<String> {
///     Ok(format!("approved by {}", approval.approver))
/// }
///
/// // The function is kept, and `ApprovedEvent` describes the event:
/// assert_eq!(ApprovedEvent::name(), "order_approved");
///
/// // A running workflow receives the event by waiting for it:
/// #[workflow]
/// async fn order(ctx: WorkflowContext, order_id: String) -> Result<String> {
///     let approval: Approval = ctx.wait_for_event(ApprovedEvent::name()).await?;
///     approved(approval).await
/// }
/// ```
#[proc_macro_attribute]
pub fn event(attr: TokenStream, item: TokenStream) -> TokenStream {
    event::event_impl(attr, item)
}

/// Marks a struct as a stateful actor type.
///
/// Each actor instance is addressed by a key, and its exclusive operations run one at a
/// time per key (single writer). Define the operations with `#[operations]`.
///
/// # Attributes
///
/// - `name`: actor type name; defaults to the struct name.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[actor]
/// pub struct ShoppingCart;
///
/// #[actor(name = "CustomCart")]
/// pub struct Cart;
///
/// assert_eq!(ShoppingCart::ACTOR_NAME, "ShoppingCart");
/// assert_eq!(Cart::ACTOR_NAME, "CustomCart");
/// ```
///
/// # Generated Code
///
/// - An `Actor` trait implementation with an `actor_name()` method
/// - An `ACTOR_NAME` constant holding the actor type name
///
/// # Usage with Operations
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// pub struct Item {
///     name: String,
/// }
///
/// #[actor]
/// pub struct ShoppingCart;
///
/// #[operations]
/// impl ShoppingCart {
///     #[operation(exclusive)]
///     pub async fn add_item(ctx: ActorContext, item: Item) -> Result<usize> {
///         // Add the item and return the new item count.
///         let state = ctx.state();
///         let mut items: Vec<Item> = state.get("items").await?.unwrap_or_default();
///         items.push(item);
///         state.set("items", &items).await?;
///         Ok(items.len())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn actor(attr: TokenStream, item: TokenStream) -> TokenStream {
    actor::actor_impl(attr, item)
}

/// Defines the operations of an actor on its impl block.
///
/// Generates a typed client call for each operation, with serialization and dispatch
/// handled for you.
///
/// # Operation Attributes
///
/// Mark each operation method with one of:
///
/// - `#[operation(exclusive)]`: single-writer; runs one at a time per actor key.
/// - `#[operation(shared)]`: read-only. Shared operations are currently serialized like
///   exclusive ones.
///
/// Methods without `#[operation]` are kept as ordinary methods.
///
/// # Method Requirements
///
/// - First parameter must be `ActorContext` (exclusive) or `SharedActorContext` (shared).
/// - Must be `async`.
/// - Must return `Result<T, E>` where `T` is serializable.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Default, Serialize, Deserialize)]
/// pub struct Cart {
///     items: Vec<String>,
/// }
///
/// #[actor]
/// pub struct ShoppingCart;
///
/// #[operations]
/// impl ShoppingCart {
///     #[operation(exclusive)]
///     pub async fn add_item(ctx: ActorContext, item: String) -> Result<Cart> {
///         let state = ctx.state();
///         let mut cart = state.get::<Cart>("cart").await?.unwrap_or_default();
///         cart.items.push(item);
///         state.set("cart", &cart).await?;
///         Ok(cart)
///     }
///
///     #[operation(shared)]
///     pub async fn view_cart(ctx: SharedActorContext) -> Result<Cart> {
///         Ok(ctx.state().get::<Cart>("cart").await?.unwrap_or_default())
///     }
/// }
///
/// assert_eq!(ShoppingCart::operations().len(), 2);
///
/// // Callers use the generated typed client:
/// async fn add(client: &Client) -> Result<Cart> {
///     client.actor::<ShoppingCart>("user-123").add_item("tea".to_string()).await
/// }
/// ```
///
/// # Generated Code
///
/// For each operation:
/// - A static method for client calls: `ShoppingCart::add_item(key, item).await`
/// - Dispatcher integration with serialization and deserialization
/// - An internal `*_impl` method holding the original body
///
/// It also generates `ShoppingCart::operations()`, which returns metadata for every
/// operation.
#[proc_macro_attribute]
pub fn operations(attr: TokenStream, item: TokenStream) -> TokenStream {
    actor::operations_impl(attr, item)
}

/// Marks a method in an `#[operations]` block as an actor operation.
///
/// The mode is required: `#[operation(exclusive)]` for a single-writer operation or
/// `#[operation(shared)]` for a read-only one. On its own this attribute does nothing;
/// `#[operations]` reads it.
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[actor]
/// pub struct Counter;
///
/// #[operations]
/// impl Counter {
///     #[operation(exclusive)]
///     pub async fn add(ctx: ActorContext, delta: i64) -> Result<i64> {
///         // Changes actor state.
///         let state = ctx.state();
///         let count = state.get::<i64>("count").await?.unwrap_or(0) + delta;
///         state.set("count", &count).await?;
///         Ok(count)
///     }
///
///     #[operation(shared)]
///     pub async fn read(ctx: SharedActorContext) -> Result<i64> {
///         // Reads actor state only.
///         Ok(ctx.state().get::<i64>("count").await?.unwrap_or(0))
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn operation(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // A marker read by `#[operations]`; the item passes through unchanged.
    item
}

/// Derives JSON payload conversion methods for a type.
///
/// The type must implement serde's `Serialize` and `Deserialize`. The derive works on
/// structs and enums, not unions. It adds inherent methods only; the `PayloadCodec` trait
/// already applies to the type through the SDK's blanket implementation.
///
/// # Generated Methods
///
/// - `to_payload(&self) -> Result<Payload, Error>`
/// - `from_payload(payload: &Payload) -> Result<Self, Error>`
/// - `try_into_payload(self) -> Result<Payload, Error>`
///
/// # Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Payload, Serialize, Deserialize)]
/// struct OrderInput {
///     order_id: String,
///     amount_cents: u64,
/// }
///
/// // Round-trip the value through a payload:
/// let input = OrderInput {
///     order_id: "order-123".to_string(),
///     amount_cents: 9_999,
/// };
/// let payload = input.to_payload()?;
/// let decoded = OrderInput::from_payload(&payload)?;
/// assert_eq!(decoded.order_id, "order-123");
/// # Ok::<(), orcher_sdk::Error>(())
/// ```
///
/// # Integration with Macros
///
/// Such types can be used directly as `#[task]` and `#[workflow]` inputs and outputs:
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[derive(Payload, Serialize, Deserialize)]
/// struct MyInput {
///     value: String,
/// }
///
/// #[task]
/// async fn my_task(_ctx: TaskContext, input: MyInput) -> Result<String> {
///     // The generated handler decodes `MyInput` from the task's payload.
///     Ok(input.value)
/// }
/// ```
#[cfg(feature = "derive")]
#[proc_macro_derive(Payload)]
pub fn derive_payload(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match derive::payload::derive_payload(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
