//! Workflow time, read from the journal.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Time helpers for workflow code, obtained from `ctx.time()`.
///
/// Reading `SystemTime::now()` or `Utc::now()` in workflow code gives a different value
/// on every replay. This clock is read from the journal instead. It starts at the moment
/// the engine journaled the workflow's start, and moves forward only when the workflow
/// receives something it waited for: a task's result, a fired timer, a child's outcome or
/// an event. It then reads as the moment the engine journaled that.
///
/// A closure run with `ctx.execute` does not move it: the closure runs inside the
/// activation and its result is journaled only afterwards, so a replay would read a
/// later time after it than the run that executed it.
///
/// Every value comes from the journal, so a replay reads the same time at each point in
/// the code as the original run, on any machine and however much later. Code that reads
/// the time inside one branch of a `join` may see a sibling branch's result move it,
/// depending on which branches had resolved.
///
/// Clones share one clock.
///
/// # Examples
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// # fn example(ctx: &WorkflowContext) {
/// // The workflow's current time; the same at this point on every replay.
/// let order_time = ctx.time().now();
///
/// // Time since the workflow started, on the same clock.
/// let elapsed = ctx.time().elapsed();
/// let elapsed_secs = ctx.time().elapsed_secs();
/// # }
/// ```
///
/// Non-deterministic:
///
/// ```rust
/// use std::time::SystemTime;
///
/// // A different timestamp on every replay.
/// let timestamp = SystemTime::now()
///     .duration_since(SystemTime::UNIX_EPOCH)
///     .unwrap()
///     .as_secs();
/// ```
///
/// Deterministic:
///
/// ```rust
/// # use orcher_sdk::prelude::*;
/// # fn example(ctx: &WorkflowContext) {
/// // The same timestamp on every replay.
/// let timestamp = ctx.time().now();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WorkflowTime {
    /// Workflow start time, in milliseconds since the UNIX epoch.
    started_at_ms: i64,
    /// The workflow's current time, in milliseconds since the UNIX epoch. Shared so
    /// that every clone, and the context state, sees the clock move.
    now_ms: Arc<AtomicI64>,
}

impl WorkflowTime {
    /// Creates a clock for a workflow that started at `started_at` (seconds since the
    /// UNIX epoch). It reads `started_at` until moved.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    ///
    /// let time = WorkflowTime::new(1704067200);
    /// assert_eq!(time.now(), 1704067200);
    /// ```
    pub fn new(started_at: i64) -> Self {
        Self::from_millis(started_at.saturating_mul(1000))
    }

    /// A clock starting at `started_at_ms`, in milliseconds since the UNIX epoch.
    pub(crate) fn from_millis(started_at_ms: i64) -> Self {
        Self {
            started_at_ms,
            now_ms: Arc::new(AtomicI64::new(started_at_ms)),
        }
    }

    /// Moves the clock forward to `at_ms`. A time earlier than the clock's is ignored, so
    /// results received out of journal order cannot move it back.
    pub(crate) fn advance_to(&self, at_ms: i64) {
        self.now_ms.fetch_max(at_ms, Ordering::SeqCst);
    }

    fn now_millis(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    /// Returns the workflow's current time, in seconds since the UNIX epoch.
    ///
    /// That is when the engine journaled the latest thing the workflow has waited for and
    /// received, or its start before it has received anything. It does not change while
    /// the workflow code runs between two such points.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    ///
    /// let time = WorkflowTime::new(1704067200);
    ///
    /// assert_eq!(time.now(), 1704067200);
    /// assert_eq!(time.now(), 1704067200); // Unchanged on later calls.
    /// ```
    ///
    /// Stamping a record:
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use serde::{Serialize, Deserialize};
    /// # #[derive(Serialize, Deserialize)]
    /// # struct Order { order_id: String, created_at: i64 }
    /// # async fn example(ctx: &WorkflowContext) -> Result<Order> {
    /// let order = Order {
    ///     order_id: format!("ORD-{}", ctx.rand().uuid()),
    ///     created_at: ctx.time().now(), // From the journal, not the clock.
    /// };
    /// # Ok(order)
    /// # }
    /// ```
    pub fn now(&self) -> i64 {
        self.now_millis().div_euclid(1000)
    }

    /// Returns the workflow start time, in seconds since the UNIX epoch. Unlike
    /// [`now`](Self::now), it never moves.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    ///
    /// let time = WorkflowTime::new(1704067200);
    ///
    /// assert_eq!(time.started_at(), time.now());
    /// ```
    pub fn started_at(&self) -> i64 {
        self.started_at_ms.div_euclid(1000)
    }

    /// Returns the time from the workflow's start to [`now`](Self::now).
    ///
    /// Measured on the workflow's clock, not the wall clock, so it is the same at each
    /// point on every replay and a branch may depend on it. It grows only as the workflow
    /// receives what it waited for.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    /// use std::time::Duration;
    ///
    /// let time = WorkflowTime::new(1704067200);
    ///
    /// // Nothing has been received yet.
    /// assert_eq!(time.elapsed(), Duration::ZERO);
    /// ```
    ///
    /// Checking a deadline:
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # use std::time::Duration;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// if ctx.time().elapsed() > Duration::from_secs(3600) {
    ///     return Err(Error::Workflow(orcher_sdk::error::WorkflowError::StateError(
    ///         "Workflow exceeded 1 hour timeout".to_string()
    ///     )));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn elapsed(&self) -> Duration {
        let elapsed_ms = self.now_millis().saturating_sub(self.started_at_ms);
        Duration::from_millis(elapsed_ms.max(0) as u64)
    }

    /// Returns [`elapsed`](Self::elapsed) in whole seconds.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    ///
    /// let time = WorkflowTime::new(1704067200);
    /// assert_eq!(time.elapsed_secs(), 0);
    /// ```
    ///
    /// Logging a long-running workflow:
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// if ctx.time().elapsed_secs() > 3600 {
    ///     tracing::warn!("Workflow has been running for over an hour");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn elapsed_secs(&self) -> u64 {
        self.elapsed().as_secs()
    }

    /// Returns [`elapsed`](Self::elapsed) in milliseconds.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::workflow::WorkflowTime;
    ///
    /// let time = WorkflowTime::new(1704067200);
    /// assert_eq!(time.elapsed_millis(), 0);
    /// ```
    ///
    /// Timing a task, from its scheduling point to its journaled result:
    ///
    /// ```rust
    /// # use orcher_sdk::prelude::*;
    /// # async fn example(ctx: &WorkflowContext) -> Result<()> {
    /// let start_ms = ctx.time().elapsed_millis();
    ///
    /// let _: String = ctx.execute_task("heavy_operation", "input").await?;
    ///
    /// let end_ms = ctx.time().elapsed_millis();
    /// tracing::info!("Operation took {}ms", end_ms - start_ms);
    /// # Ok(())
    /// # }
    /// ```
    pub fn elapsed_millis(&self) -> u128 {
        self.elapsed().as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: i64 = 1_704_067_200; // 2024-01-01 00:00:00 UTC

    #[test]
    fn a_new_clock_reads_its_start() {
        let time = WorkflowTime::new(START);
        assert_eq!(time.now(), START);
        assert_eq!(time.started_at(), START);
        assert_eq!(time.elapsed(), Duration::ZERO);
    }

    #[test]
    fn the_clock_does_not_follow_the_wall_clock() {
        let time = WorkflowTime::new(START);
        std::thread::sleep(Duration::from_millis(1_100));
        assert_eq!(time.now(), START);
        assert_eq!(time.elapsed(), Duration::ZERO);
    }

    #[test]
    fn advancing_moves_now_and_elapsed_but_not_the_start() {
        let time = WorkflowTime::new(START);
        time.advance_to((START + 90) * 1000 + 250);
        assert_eq!(time.now(), START + 90);
        assert_eq!(time.started_at(), START);
        assert_eq!(time.elapsed(), Duration::from_millis(90_250));
        assert_eq!(time.elapsed_secs(), 90);
        assert_eq!(time.elapsed_millis(), 90_250);
    }

    #[test]
    fn the_clock_never_moves_back() {
        let time = WorkflowTime::new(START);
        time.advance_to((START + 60) * 1000);
        time.advance_to((START + 10) * 1000);
        time.advance_to((START - 10) * 1000);
        assert_eq!(time.now(), START + 60);
    }

    #[test]
    fn clones_share_one_clock() {
        let time = WorkflowTime::new(START);
        let clone = time.clone();
        time.advance_to((START + 5) * 1000);
        assert_eq!(clone.now(), START + 5);
    }

    #[test]
    fn times_before_the_epoch_round_down() {
        let time = WorkflowTime::from_millis(-1_500);
        assert_eq!(time.now(), -2);
        assert_eq!(time.started_at(), -2);
    }

    #[test]
    fn debug_names_the_start() {
        let debug = format!("{:?}", WorkflowTime::new(START));
        assert!(debug.contains("WorkflowTime"));
        assert!(debug.contains("started_at"));
    }
}
