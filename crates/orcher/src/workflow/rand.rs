//! Deterministic randomness for workflows.
//!
//! Every value is derived from the workflow ID and a call counter, so a replay of the same
//! workflow produces the same values in the same order.

use rand::distributions::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// Deterministic random source for workflow code, available as `ctx.rand()`.
///
/// Workflow code is replayed, so it must produce the same values every time it runs.
/// `Uuid::new_v4()` or `rand::random()` return new values on replay and break that rule;
/// use this type instead.
///
/// # Determinism
///
/// Each call seeds a fresh RNG from a hash of the workflow ID and a call counter, then
/// increments the counter. The same workflow therefore gets the same sequence on every
/// replay, different workflows get different sequences, and successive calls within one
/// workflow return different values. Because the sequence depends on call order, workflow
/// code must make its random calls in the same order on every run. Clones share the
/// counter.
///
/// The seed uses the standard library's `DefaultHasher` and the `rand` crate's `StdRng`,
/// neither of which guarantees the same output across releases. Upgrading Rust or `rand`
/// can therefore change the values a replay produces.
///
/// # Security
///
/// **This RNG is not cryptographically secure.** Anyone who knows the workflow ID can
/// reproduce its output. Never use it for keys, tokens, or other secrets; generate those
/// in a task with a cryptographic library.
///
/// # Examples
///
/// ```rust
/// # use orcher::prelude::*;
/// # async fn example(ctx: &WorkflowContext) -> Result<()> {
/// // Deterministic UUID (same on replay)
/// let order_id = format!("ORD-{}", ctx.rand().uuid());
/// let confirmation = format!("CONF-{}", ctx.rand().uuid());
///
/// // Deterministic random number [0.0, 1.0)
/// let priority = ctx.rand().random();
///
/// // Random value in a range
/// let discount = ctx.rand().random_range(5..=20);
///
/// // Random boolean (50/50)
/// let should_expedite = ctx.rand().bool();
///
/// // Random selection from slice
/// let shipping_method = ctx.rand().choose(&["standard", "express", "overnight"]);
/// # Ok(())
/// # }
/// ```
///
/// ## Wrong: non-deterministic
///
/// ```rust
/// use uuid::Uuid;
/// use rand::random;
///
/// // Different on every run, so replay breaks.
/// let id = Uuid::new_v4();
/// let value = random::<f64>();
/// ```
///
/// ## Correct: deterministic
///
/// ```rust
/// # use orcher::prelude::*;
/// # fn example(ctx: &WorkflowContext) {
/// // Same on every run, so replay is safe.
/// let id = ctx.rand().uuid();
/// let value = ctx.rand().random();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WorkflowRand {
    /// Workflow ID that seeds every value.
    workflow_id: String,
    /// Number of values drawn so far; shared by clones so they continue one sequence.
    step_counter: Arc<AtomicU64>,
}

impl WorkflowRand {
    /// Creates a random source for the given workflow ID, starting at counter 0.
    ///
    /// Workflow code normally uses `ctx.rand()` rather than creating one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    /// let uuid = rand.uuid();
    /// ```
    pub fn new(workflow_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            step_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns an RNG seeded for the current counter value and advances the counter.
    fn next_rng(&self) -> StdRng {
        let counter = self.step_counter.fetch_add(1, Ordering::SeqCst);
        let seed = self.compute_seed(counter);
        StdRng::seed_from_u64(seed)
    }

    /// Hashes the workflow ID and `counter` into a seed.
    fn compute_seed(&self, counter: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.workflow_id.hash(&mut hasher);
        counter.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns a deterministic UUID.
    ///
    /// Successive calls return different UUIDs; a replay returns the same ones in the same
    /// order. Useful for idempotency keys, order IDs, and correlation IDs.
    ///
    /// The UUID is built from 128 random bits, so its version and variant bits are not set
    /// to those of a v4 UUID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let id1 = rand.uuid();
    /// let id2 = rand.uuid();
    ///
    /// // Successive calls differ.
    /// assert_ne!(id1, id2);
    ///
    /// // A fresh source for the same workflow repeats the sequence.
    /// let rand2 = WorkflowRand::new("wf-123");
    /// let id3 = rand2.uuid();
    /// assert_eq!(id1, id3); // Same workflow ID, and both counters started at 0
    /// ```
    ///
    /// # Example: Idempotency Key
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// // The key is the same on every replay.
    /// let idempotency_key = format!("PAY-{}", ctx.rand().uuid());
    ///
    /// // So the task receives the same key each time it is scheduled.
    /// let payment_id: String = ctx.execute_task(
    ///     "process_payment",
    ///     serde_json::json!({
    ///         "amount": 100.0,
    ///         "idempotency_key": idempotency_key
    ///     })
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn uuid(&self) -> Uuid {
        let mut rng = self.next_rng();
        let random_u128 = rng.gen();
        Uuid::from_u128(random_u128)
    }

    /// Returns a deterministic random `f64` in `[0.0, 1.0)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let value = rand.random();
    /// assert!(value >= 0.0 && value < 1.0);
    /// ```
    ///
    /// # Example: Weighted Routing
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let priority = ctx.rand().random();
    ///
    /// let queue = if priority < 0.2 {
    ///     "high-priority"
    /// } else if priority < 0.7 {
    ///     "normal"
    /// } else {
    ///     "low-priority"
    /// };
    ///
    /// let _ticket: String = ctx.execute_task("route_to_queue", queue).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn random(&self) -> f64 {
        let mut rng = self.next_rng();
        rng.gen()
    }

    /// Returns a deterministic random value from `range`, such as `5..20` or `5..=20`.
    ///
    /// # Panics
    ///
    /// Panics if `range` is empty.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// // Exclusive range
    /// let value = rand.random_range(10..20);
    /// assert!(value >= 10 && value < 20);
    ///
    /// // Inclusive range
    /// let value = rand.random_range(10..=20);
    /// assert!(value >= 10 && value <= 20);
    /// ```
    ///
    /// # Example: Random Discount
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let discount_percentage = ctx.rand().random_range(5..=20);
    /// let discount_code = format!("SAVE{}-{}", discount_percentage, ctx.rand().uuid());
    /// # Ok(())
    /// # }
    /// ```
    pub fn random_range<T, R>(&self, range: R) -> T
    where
        T: SampleUniform,
        R: rand::distributions::uniform::SampleRange<T>,
    {
        let mut rng = self.next_rng();
        rng.gen_range(range)
    }

    /// Returns a deterministic random boolean, `true` and `false` equally likely.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let coin_flip = rand.bool();
    /// assert!(coin_flip == true || coin_flip == false);
    /// ```
    ///
    /// # Example: A/B Test
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext, order_id: String) -> Result<String> {
    /// let use_new_feature = ctx.rand().bool();
    ///
    /// let receipt: String = if use_new_feature {
    ///     ctx.execute_task("new_payment_flow", order_id).await?
    /// } else {
    ///     ctx.execute_task("old_payment_flow", order_id).await?
    /// };
    /// # Ok(receipt)
    /// # }
    /// ```
    pub fn bool(&self) -> bool {
        let mut rng = self.next_rng();
        rng.gen()
    }

    /// Returns a deterministically chosen element of `items`, or `None` if it is empty.
    ///
    /// An empty slice does not advance the counter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let items = vec!["apple", "banana", "cherry"];
    /// let choice = rand.choose(&items);
    /// assert!(choice.is_some());
    ///
    /// let empty: Vec<&str> = vec![];
    /// let choice = rand.choose(&empty);
    /// assert!(choice.is_none());
    /// ```
    ///
    /// # Example: Random Server Selection
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let servers = vec!["server1.com", "server2.com", "server3.com"];
    /// let server = ctx
    ///     .rand()
    ///     .choose(&servers)
    ///     .ok_or_else(|| Error::Other("no servers available".to_string()))?;
    ///
    /// let _response: String = ctx
    ///     .execute_task("call_api", serde_json::json!({ "server": server }))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn choose<'a, T>(&self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        let mut rng = self.next_rng();
        let index = rng.gen_range(0..items.len());
        Some(&items[index])
    }

    /// Shuffles `items` in place; a replay produces the same order.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let mut items = vec![1, 2, 3, 4, 5];
    /// rand.shuffle(&mut items);
    /// // The order is random but the same on every replay.
    /// ```
    ///
    /// # Example: Process Items in Random Order
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let mut items = vec!["item1", "item2", "item3", "item4"];
    /// ctx.rand().shuffle(&mut items);
    ///
    /// for item in items {
    ///     let _processed: bool = ctx.execute_task("process_item", item).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn shuffle<T>(&self, items: &mut [T]) {
        use rand::seq::SliceRandom;
        let mut rng = self.next_rng();
        items.shuffle(&mut rng);
    }

    /// Returns `count` deterministically chosen elements of `items`, without replacement.
    ///
    /// No element position is picked twice. If `count` exceeds the slice length, every
    /// element is returned. An empty slice returns an empty vector without advancing the
    /// counter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    ///
    /// let items = vec![1, 2, 3, 4, 5];
    /// let chosen = rand.choose_multiple(&items, 3);
    /// assert_eq!(chosen.len(), 3);
    ///
    /// // Asking for more than the slice holds returns every element.
    /// let chosen = rand.choose_multiple(&items, 10);
    /// assert_eq!(chosen.len(), 5);
    /// ```
    ///
    /// # Example: Process a Random Subset
    ///
    /// ```rust
    /// # use orcher::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let all_users = vec!["user1", "user2", "user3", "user4", "user5"];
    /// let sample = ctx.rand().choose_multiple(&all_users, 2);
    ///
    /// for user in sample {
    ///     let _sent: bool = ctx.execute_task("send_survey", user).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn choose_multiple<T: Clone>(&self, items: &[T], count: usize) -> Vec<T> {
        use rand::seq::SliceRandom;

        if items.is_empty() {
            return vec![];
        }

        let mut rng = self.next_rng();
        let actual_count = count.min(items.len());
        items
            .choose_multiple(&mut rng, actual_count)
            .cloned()
            .collect()
    }

    /// Returns how many random values have been drawn, across this source and its clones.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher::workflow::WorkflowRand;
    ///
    /// let rand = WorkflowRand::new("wf-123");
    /// assert_eq!(rand.step_count(), 0);
    ///
    /// rand.uuid();
    /// assert_eq!(rand.step_count(), 1);
    ///
    /// rand.random();
    /// assert_eq!(rand.step_count(), 2);
    /// ```
    pub fn step_count(&self) -> u64 {
        self.step_counter.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_rand_uuid_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");

        let uuid1 = rand1.uuid();
        let uuid2 = rand2.uuid();

        // Same workflow ID, same UUID.
        assert_eq!(uuid1, uuid2);
    }

    #[test]
    fn test_workflow_rand_uuid_different_workflows() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-456");

        let uuid1 = rand1.uuid();
        let uuid2 = rand2.uuid();

        // Different workflow IDs, different UUIDs.
        assert_ne!(uuid1, uuid2);
    }

    #[test]
    fn test_workflow_rand_uuid_sequence() {
        let rand = WorkflowRand::new("wf-123");

        let uuid1 = rand.uuid();
        let uuid2 = rand.uuid();
        let uuid3 = rand.uuid();

        // Each call advances the counter, so all differ.
        assert_ne!(uuid1, uuid2);
        assert_ne!(uuid2, uuid3);
        assert_ne!(uuid1, uuid3);
    }

    #[test]
    fn test_workflow_rand_random_in_range() {
        let rand = WorkflowRand::new("wf-123");

        for _ in 0..100 {
            let value = rand.random();
            assert!((0.0..1.0).contains(&value));
        }
    }

    #[test]
    fn test_workflow_rand_random_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");

        let value1 = rand1.random();
        let value2 = rand2.random();

        assert_eq!(value1, value2);
    }

    #[test]
    fn test_workflow_rand_random_range() {
        let rand = WorkflowRand::new("wf-123");

        for _ in 0..100 {
            let value = rand.random_range(10..20);
            assert!((10..20).contains(&value));
        }
    }

    #[test]
    fn test_workflow_rand_random_range_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");

        let value1 = rand1.random_range(5..15);
        let value2 = rand2.random_range(5..15);

        assert_eq!(value1, value2);
    }

    #[test]
    fn test_workflow_rand_bool() {
        let rand = WorkflowRand::new("wf-123");

        let mut true_count = 0;
        let mut false_count = 0;

        for _ in 0..100 {
            if rand.bool() {
                true_count += 1;
            } else {
                false_count += 1;
            }
        }

        // Both values occur.
        assert!(true_count > 0);
        assert!(false_count > 0);
    }

    #[test]
    fn test_workflow_rand_bool_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");

        let value1 = rand1.bool();
        let value2 = rand2.bool();

        assert_eq!(value1, value2);
    }

    #[test]
    fn test_workflow_rand_choose() {
        let rand = WorkflowRand::new("wf-123");
        let items = vec!["a", "b", "c"];

        let choice = rand.choose(&items);
        assert!(choice.is_some());
        assert!(items.contains(choice.unwrap()));
    }

    #[test]
    fn test_workflow_rand_choose_empty() {
        let rand = WorkflowRand::new("wf-123");
        let items: Vec<&str> = vec![];

        let choice = rand.choose(&items);
        assert!(choice.is_none());
    }

    #[test]
    fn test_workflow_rand_choose_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");
        let items = vec!["a", "b", "c"];

        let choice1 = rand1.choose(&items);
        let choice2 = rand2.choose(&items);

        assert_eq!(choice1, choice2);
    }

    #[test]
    fn test_workflow_rand_shuffle() {
        let rand = WorkflowRand::new("wf-123");
        let mut items1 = vec![1, 2, 3, 4, 5];
        let items2 = items1.clone();

        rand.shuffle(&mut items1);

        // A shuffle may in principle return the original order; for this fixed seed it
        // does not.
        let same_order = items1.iter().zip(items2.iter()).all(|(a, b)| a == b);
        assert!(!same_order || items1.len() <= 1);
    }

    #[test]
    fn test_workflow_rand_shuffle_deterministic() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = WorkflowRand::new("wf-123");

        let mut items1 = vec![1, 2, 3, 4, 5];
        let mut items2 = vec![1, 2, 3, 4, 5];

        rand1.shuffle(&mut items1);
        rand2.shuffle(&mut items2);

        // Same workflow ID, same shuffle.
        assert_eq!(items1, items2);
    }

    #[test]
    fn test_workflow_rand_shuffle_empty() {
        let rand = WorkflowRand::new("wf-123");
        let mut items: Vec<i32> = vec![];

        rand.shuffle(&mut items);
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_workflow_rand_shuffle_single() {
        let rand = WorkflowRand::new("wf-123");
        let mut items = vec![42];

        rand.shuffle(&mut items);
        assert_eq!(items, vec![42]);
    }

    #[test]
    fn test_workflow_rand_choose_multiple() {
        let rand = WorkflowRand::new("wf-123");
        let items = vec![1, 2, 3, 4, 5];

        let chosen = rand.choose_multiple(&items, 3);
        assert_eq!(chosen.len(), 3);

        // Every chosen item comes from the slice.
        for item in &chosen {
            assert!(items.contains(item));
        }

        // No duplicates.
        let mut sorted = chosen.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), chosen.len());
    }

    #[test]
    fn test_workflow_rand_choose_multiple_more_than_available() {
        let rand = WorkflowRand::new("wf-123");
        let items = vec![1, 2, 3];

        let chosen = rand.choose_multiple(&items, 10);
        assert_eq!(chosen.len(), 3); // Every element is returned
    }

    #[test]
    fn test_workflow_rand_choose_multiple_empty() {
        let rand = WorkflowRand::new("wf-123");
        let items: Vec<i32> = vec![];

        let chosen = rand.choose_multiple(&items, 5);
        assert_eq!(chosen.len(), 0);
    }

    #[test]
    fn test_workflow_rand_clone() {
        let rand1 = WorkflowRand::new("wf-123");
        let rand2 = rand1.clone();

        // Clones share one counter, so the second call continues the sequence.
        let uuid1 = rand1.uuid();
        let uuid2 = rand2.uuid();

        assert_ne!(uuid1, uuid2);

        // Independent sources for the same workflow start at the same point.
        let rand3 = WorkflowRand::new("wf-123");
        let rand4 = WorkflowRand::new("wf-123");
        let uuid3 = rand3.uuid();
        let uuid4 = rand4.uuid();
        assert_eq!(uuid3, uuid4);
    }

    #[test]
    fn test_workflow_rand_compute_seed_consistency() {
        let rand = WorkflowRand::new("wf-123");

        let seed1 = rand.compute_seed(0);
        let seed2 = rand.compute_seed(0);

        // Same counter, same seed.
        assert_eq!(seed1, seed2);
    }

    #[test]
    fn test_workflow_rand_compute_seed_different_counters() {
        let rand = WorkflowRand::new("wf-123");

        let seed1 = rand.compute_seed(0);
        let seed2 = rand.compute_seed(1);

        // Different counters, different seeds.
        assert_ne!(seed1, seed2);
    }

    #[test]
    fn test_workflow_rand_step_count() {
        let rand = WorkflowRand::new("wf-123");

        assert_eq!(rand.step_count(), 0);

        rand.uuid();
        assert_eq!(rand.step_count(), 1);

        rand.random();
        assert_eq!(rand.step_count(), 2);

        rand.random_range(1..10);
        assert_eq!(rand.step_count(), 3);

        rand.bool();
        assert_eq!(rand.step_count(), 4);
    }
}
