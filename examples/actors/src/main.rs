//! # Actors Service
//!
//! This service demonstrates virtual actors with single-writer-per-key guarantees.
//!
//! ## Key Concepts
//!
//! **Actors**: Stateful virtual objects with single-writer-per-key semantics
//! - Each actor instance identified by unique key (e.g., "user_123")
//! - Operations on same key are serialized (no race conditions)
//! - Operations on different keys run in parallel
//! - State persisted automatically
//!
//! ## Architecture
//!
//! ```text
//! #[actor]                          // Define actor type
//! pub struct Counter;
//!
//! #[operations]                     // Define operations
//! impl Counter {
//!     #[operation(exclusive)]       // Write operation
//!     pub async fn increment(ctx: ActorContext, delta: i64) -> Result<i64> {
//!         let count: i64 = ctx.state().get("count").await?.unwrap_or(0);
//!         let new_count = count + delta;
//!         ctx.state().set("count", &new_count).await?;
//!         Ok(new_count)
//!     }
//!
//!     #[operation(shared)]          // Read-only operation
//!     pub async fn get_count(ctx: ActorContext) -> Result<i64> {
//!         let count: i64 = ctx.state().get("count").await?.unwrap_or(0);
//!         Ok(count)
//!     }
//! }
//! ```
//!
//! ## Usage
//!
//! ```bash
//! # Run a worker (from the repository root)
//! cargo run -p orcher-example-actors
//!
//! # Call the actors from a client
//! cargo run -p orcher-example-actors -- --client
//!
//! # Run a worker and a client in one process
//! cargo run -p orcher-example-actors -- --e2e
//! ```

use clap::Parser;
use orcher_sdk::client::ClientConfig;
use orcher_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, Level};

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug, Clone)]
#[command(name = "actors-service")]
#[command(about = "ORCHER Actors Example Service")]
struct Args {
    #[arg(long)]
    client: bool,

    #[arg(long)]
    e2e: bool,

    #[arg(
        long,
        env = "ORCHER_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server_url: String,

    #[arg(long, env = "ORCHER_TASK_QUEUE", default_value = "actors-queue")]
    task_queue: String,

    #[arg(long, env = "ORCHER_NAMESPACE", default_value = "examples")]
    namespace: String,
}

// ============================================================================
// Data Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CartItem {
    pub product_id: String,
    pub name: String,
    pub quantity: u32,
    pub price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Cart {
    pub items: Vec<CartItem>,
}

impl Cart {
    pub fn total(&self) -> f64 {
        self.items
            .iter()
            .map(|item| item.price * item.quantity as f64)
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionData {
    pub user_id: String,
    pub metadata: HashMap<String, String>,
    pub start_time: String,
    pub last_activity: String,
}

// ============================================================================
// Pattern 1: Counter Actor
// ============================================================================

/// Counter actor with increment/decrement operations
///
/// This demonstrates:
/// - Exclusive operations (modify state)
/// - Shared operations (read-only)
/// - State persistence with ctx.state()
#[actor]
pub struct Counter;

#[operations]
impl Counter {
    /// Increment the counter by a delta value (exclusive operation)
    #[operation(exclusive)]
    pub async fn increment(ctx: ActorContext, delta: i64) -> Result<i64> {
        let current: i64 = ctx.state().get("count").await?.unwrap_or(0);
        let new_value = current + delta;
        ctx.state().set("count", &new_value).await?;

        info!(
            "🔢 Counter[{}]: {} + {} = {}",
            ctx.actor_key().key(),
            current,
            delta,
            new_value
        );

        Ok(new_value)
    }

    /// Decrement the counter by a delta value (exclusive operation)
    #[operation(exclusive)]
    pub async fn decrement(ctx: ActorContext, delta: i64) -> Result<i64> {
        let current: i64 = ctx.state().get("count").await?.unwrap_or(0);
        let new_value = current - delta;
        ctx.state().set("count", &new_value).await?;

        info!(
            "🔢 Counter[{}]: {} - {} = {}",
            ctx.actor_key().key(),
            current,
            delta,
            new_value
        );

        Ok(new_value)
    }

    /// Get the current counter value (shared operation - read-only)
    #[operation(shared)]
    pub async fn get_count(ctx: ActorContext) -> Result<i64> {
        let count: i64 = ctx.state().get("count").await?.unwrap_or(0);

        info!("📊 Counter[{}]: count = {}", ctx.actor_key().key(), count);

        Ok(count)
    }

    /// Reset the counter to zero (exclusive operation)
    #[operation(exclusive)]
    pub async fn reset(ctx: ActorContext) -> Result<()> {
        ctx.state().set("count", &0i64).await?;

        info!("🔄 Counter[{}]: reset to 0", ctx.actor_key().key());

        Ok(())
    }
}

// ============================================================================
// Pattern 2: Shopping Cart Actor
// ============================================================================

/// Shopping cart actor with add/remove/clear operations
///
/// This demonstrates:
/// - Complex state (Vec of items)
/// - Business logic (calculate total)
/// - Multiple exclusive operations
#[actor]
pub struct ShoppingCart;

#[operations]
impl ShoppingCart {
    /// Add an item to the shopping cart (exclusive operation)
    #[operation(exclusive)]
    pub async fn add_item(ctx: ActorContext, item: CartItem) -> Result<Cart> {
        let mut cart: Cart = ctx.state().get("cart").await?.unwrap_or_default();

        info!(
            "🛒 Cart[{}]: Adding {} (qty: {}, ${:.2})",
            ctx.actor_key().key(),
            item.name,
            item.quantity,
            item.price
        );

        cart.items.push(item);
        ctx.state().set("cart", &cart).await?;

        info!(
            "   Cart now has {} items, total: ${:.2}",
            cart.items.len(),
            cart.total()
        );

        Ok(cart)
    }

    /// Remove an item from the cart (exclusive operation)
    #[operation(exclusive)]
    pub async fn remove_item(ctx: ActorContext, product_id: String) -> Result<Cart> {
        let mut cart: Cart = ctx.state().get("cart").await?.unwrap_or_default();

        info!(
            "🛒 Cart[{}]: Removing product {}",
            ctx.actor_key().key(),
            product_id
        );

        cart.items.retain(|item| item.product_id != product_id);
        ctx.state().set("cart", &cart).await?;

        info!(
            "   Cart now has {} items, total: ${:.2}",
            cart.items.len(),
            cart.total()
        );

        Ok(cart)
    }

    /// Get the current cart contents (shared operation - read-only)
    #[operation(shared)]
    pub async fn get_cart(ctx: ActorContext) -> Result<Cart> {
        let cart: Cart = ctx.state().get("cart").await?.unwrap_or_default();

        info!(
            "📊 Cart[{}]: {} items, ${:.2}",
            ctx.actor_key().key(),
            cart.items.len(),
            cart.total()
        );

        Ok(cart)
    }

    /// Calculate the total price (shared operation - read-only)
    #[operation(shared)]
    pub async fn get_total(ctx: ActorContext) -> Result<f64> {
        let cart: Cart = ctx.state().get("cart").await?.unwrap_or_default();
        let total = cart.total();

        info!("💰 Cart[{}]: total = ${:.2}", ctx.actor_key().key(), total);

        Ok(total)
    }

    /// Clear the shopping cart (exclusive operation)
    #[operation(exclusive)]
    pub async fn clear(ctx: ActorContext) -> Result<()> {
        ctx.state().set("cart", &Cart::default()).await?;

        info!("🗑️  Cart[{}]: cleared", ctx.actor_key().key());

        Ok(())
    }
}

// ============================================================================
// Pattern 3: Bank Account Actor
// ============================================================================

/// Bank account actor with deposit/withdraw operations
///
/// This demonstrates:
/// - Business logic (balance checks)
/// - Error handling (insufficient funds)
/// - Financial operations
#[actor]
pub struct BankAccount;

#[operations]
impl BankAccount {
    /// Deposit money into the account (exclusive operation)
    #[operation(exclusive)]
    pub async fn deposit(ctx: ActorContext, amount: f64) -> Result<f64> {
        let balance: f64 = ctx.state().get("balance").await?.unwrap_or(0.0);
        let new_balance = balance + amount;
        ctx.state().set("balance", &new_balance).await?;

        info!(
            "💰 Account[{}]: Deposited ${:.2}, balance: ${:.2} → ${:.2}",
            ctx.actor_key().key(),
            amount,
            balance,
            new_balance
        );

        Ok(new_balance)
    }

    /// Withdraw money from the account (exclusive operation)
    #[operation(exclusive)]
    pub async fn withdraw(ctx: ActorContext, amount: f64) -> Result<f64> {
        let balance: f64 = ctx.state().get("balance").await?.unwrap_or(0.0);

        if balance < amount {
            info!(
                "❌ Account[{}]: Insufficient funds (${:.2} < ${:.2})",
                ctx.actor_key().key(),
                balance,
                amount
            );
            return Err(Error::Actor(format!(
                "Insufficient funds: have ${:.2}, need ${:.2}",
                balance, amount
            )));
        }

        let new_balance = balance - amount;
        ctx.state().set("balance", &new_balance).await?;

        info!(
            "💰 Account[{}]: Withdrew ${:.2}, balance: ${:.2} → ${:.2}",
            ctx.actor_key().key(),
            amount,
            balance,
            new_balance
        );

        Ok(new_balance)
    }

    /// Get the current balance (shared operation - read-only)
    #[operation(shared)]
    pub async fn get_balance(ctx: ActorContext) -> Result<f64> {
        let balance: f64 = ctx.state().get("balance").await?.unwrap_or(0.0);

        info!(
            "📊 Account[{}]: balance = ${:.2}",
            ctx.actor_key().key(),
            balance
        );

        Ok(balance)
    }
}

// ============================================================================
// Pattern 4: Session Actor
// ============================================================================

/// User session actor for tracking active sessions
///
/// This demonstrates:
/// - Complex state objects
/// - Time tracking
/// - Session lifecycle management
#[actor]
pub struct Session;

#[operations]
impl Session {
    /// Start a new session (exclusive operation)
    #[operation(exclusive)]
    pub async fn start(ctx: ActorContext, user_id: String) -> Result<SessionData> {
        let now = jiff::Timestamp::now().to_string();
        let session = SessionData {
            user_id: user_id.clone(),
            metadata: HashMap::new(),
            start_time: now.clone(),
            last_activity: now,
        };

        ctx.state().set("session", &session).await?;

        info!(
            "👤 Session[{}]: Started for user {}",
            ctx.actor_key().key(),
            user_id
        );

        Ok(session)
    }

    /// Update session metadata (exclusive operation)
    #[operation(exclusive)]
    pub async fn update(ctx: ActorContext, key: String, value: String) -> Result<SessionData> {
        let mut session: SessionData = ctx
            .state()
            .get("session")
            .await?
            .ok_or_else(|| Error::Actor("Session not found".to_string()))?;

        session.metadata.insert(key.clone(), value.clone());
        session.last_activity = jiff::Timestamp::now().to_string();
        ctx.state().set("session", &session).await?;

        info!(
            "👤 Session[{}]: Updated {} = {}",
            ctx.actor_key().key(),
            key,
            value
        );

        Ok(session)
    }

    /// Get session data (shared operation - read-only)
    #[operation(shared)]
    pub async fn get(ctx: ActorContext) -> Result<Option<SessionData>> {
        let session: Option<SessionData> = ctx.state().get("session").await?;

        if let Some(ref s) = session {
            info!(
                "📊 Session[{}]: Active for user {}",
                ctx.actor_key().key(),
                s.user_id
            );
        } else {
            info!("📊 Session[{}]: Not found", ctx.actor_key().key());
        }

        Ok(session)
    }

    /// End the session (exclusive operation)
    #[operation(exclusive)]
    pub async fn end(ctx: ActorContext) -> Result<()> {
        ctx.state()
            .set::<Option<SessionData>>("session", &None)
            .await?;

        info!("👋 Session[{}]: Ended", ctx.actor_key().key());

        Ok(())
    }
}

// ============================================================================
// Pattern 5: Inventory Actor
// ============================================================================

/// Inventory management actor with stock and reservations
///
/// This demonstrates:
/// - Multiple state fields
/// - Business logic (availability checks)
/// - Reservation patterns
#[actor]
pub struct Inventory;

#[operations]
impl Inventory {
    /// Add stock (exclusive operation)
    #[operation(exclusive)]
    pub async fn add_stock(ctx: ActorContext, quantity: i32) -> Result<i32> {
        let stock: i32 = ctx.state().get("stock").await?.unwrap_or(0);
        let new_stock = stock + quantity;
        ctx.state().set("stock", &new_stock).await?;

        info!(
            "📦 Inventory[{}]: Added {} units (stock: {} → {})",
            ctx.actor_key().key(),
            quantity,
            stock,
            new_stock
        );

        Ok(new_stock)
    }

    /// Reserve stock (exclusive operation)
    #[operation(exclusive)]
    pub async fn reserve(ctx: ActorContext, quantity: i32) -> Result<bool> {
        let stock: i32 = ctx.state().get("stock").await?.unwrap_or(0);
        let reserved: i32 = ctx.state().get("reserved").await?.unwrap_or(0);
        let available = stock - reserved;

        if available >= quantity {
            let new_reserved = reserved + quantity;
            ctx.state().set("reserved", &new_reserved).await?;

            info!(
                "🔒 Inventory[{}]: Reserved {} units (available: {} → {})",
                ctx.actor_key().key(),
                quantity,
                available,
                available - quantity
            );

            Ok(true)
        } else {
            info!(
                "❌ Inventory[{}]: Cannot reserve {} units (only {} available)",
                ctx.actor_key().key(),
                quantity,
                available
            );

            Ok(false)
        }
    }

    /// Release reserved stock (exclusive operation)
    #[operation(exclusive)]
    pub async fn release(ctx: ActorContext, quantity: i32) -> Result<()> {
        let reserved: i32 = ctx.state().get("reserved").await?.unwrap_or(0);
        let new_reserved = (reserved - quantity).max(0);
        ctx.state().set("reserved", &new_reserved).await?;

        info!(
            "🔓 Inventory[{}]: Released {} units (reserved: {} → {})",
            ctx.actor_key().key(),
            quantity,
            reserved,
            new_reserved
        );

        Ok(())
    }

    /// Get inventory status (shared operation - read-only)
    #[operation(shared)]
    pub async fn get_status(ctx: ActorContext) -> Result<(i32, i32, i32)> {
        let stock: i32 = ctx.state().get("stock").await?.unwrap_or(0);
        let reserved: i32 = ctx.state().get("reserved").await?.unwrap_or(0);
        let available = stock - reserved;

        info!(
            "📊 Inventory[{}]: stock={}, reserved={}, available={}",
            ctx.actor_key().key(),
            stock,
            reserved,
            available
        );

        Ok((stock, reserved, available))
    }
}

// ============================================================================
// Service Implementation
// ============================================================================

async fn run_worker(args: Args) -> anyhow::Result<()> {
    info!("╔════════════════════════════════════════════════════════════╗");
    info!("║           ORCHER Actors Example Service               ║");
    info!("╚════════════════════════════════════════════════════════════╝");
    info!("");
    info!("🔧 Starting Actors Service in WORKER mode");
    info!("   Server: {}", args.server_url);
    info!("   Task Queue: {}", args.task_queue);
    info!("   Namespace: {}", args.namespace);
    info!("");

    let worker = Worker::builder()
        .server_url(&args.server_url)
        .task_queue(&args.task_queue)
        .namespace(&args.namespace)
        .build()
        .await?;

    info!("✅ Worker registered and ready to accept work");
    info!("   Registered actors:");
    info!("   ├─ Counter (Pattern 1)");
    info!("   ├─ ShoppingCart (Pattern 2)");
    info!("   ├─ BankAccount (Pattern 3)");
    info!("   ├─ Session (Pattern 4)");
    info!("   └─ Inventory (Pattern 5)");
    info!("");
    info!("⏳ Polling for work...");
    info!("");

    worker.run().await?;
    Ok(())
}

async fn run_client(args: Args) -> anyhow::Result<()> {
    info!("📱 Starting Actors Service in CLIENT mode");
    let client =
        Client::with_config(ClientConfig::new(&args.server_url).with_namespace(&args.namespace))
            .await?;
    info!("✅ Connected to ORCHER server: {}", args.server_url);
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("Pattern 1: Counter Actor (typed client)");
    info!("═══════════════════════════════════════════════════════════");

    let counter_key = "counter_1";
    let counter = client.actor::<Counter>(counter_key);

    info!("Invoking Counter.increment(10) on key '{}'", counter_key);
    let count = counter.increment(10i64).await?;
    info!("Count: {}", count);

    info!("Invoking Counter.increment(5)");
    let count = counter.increment(5i64).await?;
    info!("Count: {}", count);

    info!("Invoking Counter.get_count() (read-only)");
    let count = counter.get_count().await?;
    info!("Count: {}", count);
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("Pattern 2: Shopping Cart Actor (typed client)");
    info!("═══════════════════════════════════════════════════════════");

    let cart_key = "cart_user123";
    let cart_client = client.actor::<ShoppingCart>(cart_key);

    let laptop = CartItem {
        product_id: "PROD-001".to_string(),
        name: "Laptop".to_string(),
        quantity: 1,
        price: 999.99,
    };

    info!("Adding laptop to cart");
    let cart = cart_client.add_item(laptop).await?;
    info!("Cart: {} items, ${:.2}", cart.items.len(), cart.total());

    let mouse = CartItem {
        product_id: "PROD-002".to_string(),
        name: "Mouse".to_string(),
        quantity: 2,
        price: 29.99,
    };

    info!("Adding mouse to cart");
    let cart = cart_client.add_item(mouse).await?;
    info!("Cart: {} items, ${:.2}", cart.items.len(), cart.total());

    info!("Getting cart total (read-only)");
    let total = cart_client.get_total().await?;
    info!("Total: ${:.2}", total);
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("Pattern 3: Bank Account Actor (typed client)");
    info!("═══════════════════════════════════════════════════════════");

    let account_key = "account_alice";
    let account = client.actor::<BankAccount>(account_key);

    info!("Depositing $1000.00");
    let balance = account.deposit(1000.0).await?;
    info!("Balance: ${:.2}", balance);

    info!("Withdrawing $300.00");
    let balance = account.withdraw(300.0).await?;
    info!("Balance: ${:.2}", balance);

    info!("Getting balance (read-only)");
    let balance = account.get_balance().await?;
    info!("Balance: ${:.2}", balance);
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("Pattern 4: Session Actor (typed client)");
    info!("═══════════════════════════════════════════════════════════");

    let session_key = "session_xyz";
    let session_client = client.actor::<Session>(session_key);

    info!("Starting session");
    let session = session_client.start("user_123".to_string()).await?;
    info!("Session started for user: {}", session.user_id);

    info!("Updating session metadata");
    let _session = session_client
        .update("browser".to_string(), "Chrome".to_string())
        .await?;
    info!("Session updated");
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("Pattern 5: Inventory Actor (typed client)");
    info!("═══════════════════════════════════════════════════════════");

    let product_key = "product_laptop";
    let inventory = client.actor::<Inventory>(product_key);

    info!("Adding 100 units to stock");
    let stock = inventory.add_stock(100i32).await?;
    info!("Stock: {}", stock);

    info!("Reserving 30 units");
    let success = inventory.reserve(30i32).await?;
    info!("Reserved: {}", success);

    info!("Getting inventory status (read-only)");
    let (stock, reserved, available) = inventory.get_status().await?;
    info!(
        "Stock: {}, Reserved: {}, Available: {}",
        stock, reserved, available
    );
    info!("");

    info!("═══════════════════════════════════════════════════════════");
    info!("📊 Summary: All Actor Patterns Demonstrated");
    info!("═══════════════════════════════════════════════════════════");
    info!("✅ Pattern 1: Counter - Increment/decrement with state");
    info!("✅ Pattern 2: Shopping Cart - Add/remove items");
    info!("✅ Pattern 3: Bank Account - Deposits/withdrawals");
    info!("✅ Pattern 4: Session - User session tracking");
    info!("✅ Pattern 5: Inventory - Stock management");
    info!("");
    info!("✅ Client mode complete - all actor patterns tested");

    Ok(())
}

async fn run_e2e(args: Args) -> anyhow::Result<()> {
    info!("🧪 Starting E2E Test Mode for Actors");
    info!("");

    let worker_args = args.clone();
    let worker_handle = tokio::spawn(async move { run_worker(worker_args).await });

    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("⏳ Worker started, running client tests...");
    info!("");

    run_client(args).await?;

    info!("");
    info!("✅ E2E test PASSED");
    info!("");
    info!("📊 E2E Test Summary:");
    info!("   ✅ All 5 actors answered their operations");
    info!("   ✅ Exclusive operations (writes) and shared operations (reads) ran");
    info!("");
    info!("Actor state lives on the engine: run the client again and the counter,");
    info!("cart and balances carry on from where this run left them.");

    worker_handle.abort();
    Ok(())
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let args = Args::parse();

    if args.e2e {
        run_e2e(args).await
    } else if args.client {
        run_client(args).await
    } else {
        run_worker(args).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cart_total() {
        let cart = Cart {
            items: vec![
                CartItem {
                    product_id: "1".to_string(),
                    name: "Item1".to_string(),
                    quantity: 2,
                    price: 10.0,
                },
                CartItem {
                    product_id: "2".to_string(),
                    name: "Item2".to_string(),
                    quantity: 1,
                    price: 5.0,
                },
            ],
        };

        assert_eq!(cart.total(), 25.0);
    }

    #[test]
    fn test_cart_item() {
        let item = CartItem {
            product_id: "PROD-001".to_string(),
            name: "Test".to_string(),
            quantity: 1,
            price: 9.99,
        };
        assert_eq!(item.quantity, 1);
    }
}
