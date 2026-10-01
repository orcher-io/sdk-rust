//! [`MockClock`], a clock that tests advance by hand instead of waiting for real time.
//!
//! # Example
//!
//! ```rust
//! use orcher_sdk::testing::MockClock;
//! use std::time::Duration;
//!
//! let clock = MockClock::new();
//!
//! assert_eq!(clock.elapsed(), Duration::ZERO);
//!
//! clock.advance(Duration::from_secs(5));
//! assert_eq!(clock.elapsed(), Duration::from_secs(5));
//!
//! clock.advance_ms(3000);
//! assert_eq!(clock.elapsed(), Duration::from_secs(8));
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

/// A clock for tests that starts at zero and moves only when advanced.
///
/// Clones share the same time and timers, and all methods are thread-safe. Reading
/// the time is lock-free; timer bookkeeping takes a mutex. Time has millisecond
/// resolution.
///
/// A timer registered with [`register_timer`](Self::register_timer) fires when time
/// reaches its deadline.
#[derive(Clone)]
pub struct MockClock {
    inner: Arc<MockClockInner>,
}

struct MockClockInner {
    /// Elapsed milliseconds; only moves forward, except on `reset`
    elapsed_ms: AtomicU64,
    /// Wakes `wait_for_advance` callers whenever time advances
    notify: Notify,
    /// Timers that have not fired yet
    timers: std::sync::Mutex<Vec<TimerEntry>>,
}

struct TimerEntry {
    deadline_ms: u64,
    notify: Arc<Notify>,
    fired: bool,
}

impl MockClock {
    /// Create a clock at time zero with no timers.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockClockInner {
                elapsed_ms: AtomicU64::new(0),
                notify: Notify::new(),
                timers: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// Time elapsed since zero.
    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(self.inner.elapsed_ms.load(Ordering::SeqCst))
    }

    /// Time elapsed since zero, in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.inner.elapsed_ms.load(Ordering::SeqCst)
    }

    /// Advance time by `duration`, truncated to whole milliseconds.
    ///
    /// Fires every timer whose deadline has been reached.
    pub fn advance(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.advance_ms(ms);
    }

    /// Advance time by `ms` milliseconds.
    ///
    /// Fires every timer whose deadline has been reached, then wakes
    /// [`wait_for_advance`](Self::wait_for_advance) callers.
    pub fn advance_ms(&self, ms: u64) {
        let new_time = self.inner.elapsed_ms.fetch_add(ms, Ordering::SeqCst) + ms;

        let mut timers = self.inner.timers.lock().unwrap();
        for timer in timers.iter_mut() {
            if !timer.fired && timer.deadline_ms <= new_time {
                timer.fired = true;
                timer.notify.notify_one();
            }
        }

        timers.retain(|t| !t.fired);

        self.inner.notify.notify_waiters();
    }

    /// Advance time to `target_ms` milliseconds since zero.
    ///
    /// Does nothing if the clock is already at or past the target.
    pub fn advance_to_ms(&self, target_ms: u64) {
        let current = self.inner.elapsed_ms.load(Ordering::SeqCst);
        if target_ms > current {
            self.advance_ms(target_ms - current);
        }
    }

    /// Register a timer that fires `delay` after the current time.
    ///
    /// The returned `Notify` is notified once, when the timer fires. It stores the
    /// permit, so a waiter that starts after the timer fired still wakes.
    pub fn register_timer(&self, delay: Duration) -> Arc<Notify> {
        let deadline_ms = self.elapsed_ms() + delay.as_millis() as u64;
        let notify = Arc::new(Notify::new());

        let mut timers = self.inner.timers.lock().unwrap();
        timers.push(TimerEntry {
            deadline_ms,
            notify: notify.clone(),
            fired: false,
        });

        notify
    }

    /// Wait until time next advances.
    ///
    /// Only an advance that happens after this future is first polled wakes it.
    pub async fn wait_for_advance(&self) {
        self.inner.notify.notified().await;
    }

    /// Number of timers that have not fired.
    pub fn pending_timer_count(&self) -> usize {
        self.inner.timers.lock().unwrap().len()
    }

    /// Deadline of the earliest pending timer, in milliseconds since zero.
    pub fn next_timer_deadline_ms(&self) -> Option<u64> {
        self.inner
            .timers
            .lock()
            .unwrap()
            .iter()
            .filter(|t| !t.fired)
            .map(|t| t.deadline_ms)
            .min()
    }

    /// Advance time to the earliest pending deadline, firing that timer.
    ///
    /// Other timers with the same deadline fire too. Returns `false` if no timer
    /// was pending.
    pub fn fire_next_timer(&self) -> bool {
        if let Some(deadline) = self.next_timer_deadline_ms() {
            self.advance_to_ms(deadline);
            true
        } else {
            false
        }
    }

    /// Advance time to the latest pending deadline, firing every pending timer.
    ///
    /// Returns the number of timers fired.
    pub fn fire_all_timers(&self) -> usize {
        let max_deadline = {
            let timers = self.inner.timers.lock().unwrap();
            timers
                .iter()
                .filter(|t| !t.fired)
                .map(|t| t.deadline_ms)
                .max()
        };

        if let Some(deadline) = max_deadline {
            let count = self.pending_timer_count();
            self.advance_to_ms(deadline);
            count
        } else {
            0
        }
    }

    /// Set the clock back to zero and drop all pending timers without firing them.
    pub fn reset(&self) {
        self.inner.elapsed_ms.store(0, Ordering::SeqCst);
        self.inner.timers.lock().unwrap().clear();
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let clock = MockClock::new();
        assert_eq!(clock.elapsed(), Duration::ZERO);
        assert_eq!(clock.elapsed_ms(), 0);
        assert_eq!(clock.pending_timer_count(), 0);
    }

    #[test]
    fn test_advance() {
        let clock = MockClock::new();
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.elapsed(), Duration::from_secs(5));

        clock.advance_ms(3000);
        assert_eq!(clock.elapsed_ms(), 8000);
    }

    #[test]
    fn test_advance_to() {
        let clock = MockClock::new();
        clock.advance_ms(5000);
        clock.advance_to_ms(10000);
        assert_eq!(clock.elapsed_ms(), 10000);

        // Advancing to a time in the past does nothing.
        clock.advance_to_ms(3000);
        assert_eq!(clock.elapsed_ms(), 10000);
    }

    #[test]
    fn test_timer_fires_on_advance() {
        let clock = MockClock::new();
        let notify = clock.register_timer(Duration::from_secs(5));

        assert_eq!(clock.pending_timer_count(), 1);

        // 3s: before the deadline
        clock.advance(Duration::from_secs(3));
        assert_eq!(clock.pending_timer_count(), 1);

        // 6s: past the deadline, so it fires
        clock.advance(Duration::from_secs(3));
        assert_eq!(clock.pending_timer_count(), 0);

        // The notification itself is not awaited here.
        let _ = notify;
    }

    #[test]
    fn test_fire_next_timer() {
        let clock = MockClock::new();
        let _t1 = clock.register_timer(Duration::from_secs(10));
        let _t2 = clock.register_timer(Duration::from_secs(5));

        assert!(clock.fire_next_timer());
        assert_eq!(clock.elapsed_ms(), 5000);
        assert_eq!(clock.pending_timer_count(), 1);

        assert!(clock.fire_next_timer());
        assert_eq!(clock.elapsed_ms(), 10000);
        assert_eq!(clock.pending_timer_count(), 0);

        assert!(!clock.fire_next_timer());
    }

    #[test]
    fn test_fire_all_timers() {
        let clock = MockClock::new();
        let _t1 = clock.register_timer(Duration::from_secs(3));
        let _t2 = clock.register_timer(Duration::from_secs(7));
        let _t3 = clock.register_timer(Duration::from_secs(5));

        let fired = clock.fire_all_timers();
        assert_eq!(fired, 3);
        assert_eq!(clock.elapsed_ms(), 7000);
        assert_eq!(clock.pending_timer_count(), 0);
    }

    #[test]
    fn test_reset() {
        let clock = MockClock::new();
        clock.advance_ms(5000);
        let _timer = clock.register_timer(Duration::from_secs(10));

        clock.reset();
        assert_eq!(clock.elapsed_ms(), 0);
        assert_eq!(clock.pending_timer_count(), 0);
    }

    #[test]
    fn test_clone_shares_state() {
        let clock = MockClock::new();
        let clock2 = clock.clone();

        clock.advance_ms(1000);
        assert_eq!(clock2.elapsed_ms(), 1000);
    }
}
