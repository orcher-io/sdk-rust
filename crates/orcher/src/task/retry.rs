//! Retry policies for tasks: attempt limits, exponential backoff and jitter.
//!
//! [`retry_with_policy`] applies a policy to any async operation.

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;

/// How retry intervals are randomized.
///
/// Randomizing the wait spreads out retries from many clients that failed at
/// the same moment, so they do not all hit the failing service together.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub enum JitterStrategy {
    /// Wait exactly the calculated interval.
    #[default]
    None,
    /// Wait a uniformly random time in `[0, interval)`.
    Full,
    /// Wait `interval / 2` plus a uniformly random time in `[0, interval / 2)`.
    Equal,
    /// Wait a uniformly random time in `[initial_interval, previous_wait * 3)`,
    /// capped at `max_interval`.
    ///
    /// This depends on the previous wait, so it is only exact through
    /// [`RetryPolicy::retry_interval_decorrelated`] and [`retry_with_policy`].
    /// [`RetryPolicy::retry_interval`] treats it as [`Full`](Self::Full).
    Decorrelated,
}

/// How a failed task is retried: how many attempts, and how long to wait between them.
///
/// The default is 3 attempts, starting at 1 second and doubling up to 60 seconds,
/// without jitter.
///
/// # Example
///
/// ```rust
/// # use orcher::task::RetryPolicy;
/// # use std::time::Duration;
/// let policy = RetryPolicy::default()
///     .with_max_attempts(5)
///     .with_initial_interval(Duration::from_secs(1))
///     .with_max_interval(Duration::from_secs(60))
///     .with_backoff_coefficient(2.0)
///     .with_non_retryable_errors(["InvalidInput"]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Maximum number of attempts, the first one included.
    ///
    /// A value of 1 means no retries.
    pub max_attempts: u32,

    /// Wait before the first retry.
    pub initial_interval: Duration,

    /// Upper bound on the wait between attempts.
    pub max_interval: Duration,

    /// Multiplier applied to the wait after each retry.
    ///
    /// With 2.0 the waits are `initial_interval`, then twice that, then four
    /// times that, and so on up to `max_interval`.
    pub backoff_coefficient: f64,

    /// How the wait is randomized. Defaults to [`JitterStrategy::None`].
    #[serde(default)]
    pub jitter: JitterStrategy,

    /// Error types that are never retried.
    ///
    /// Matched against the type a task failure is reported under: the
    /// `error_type` of a [`TaskError::application`](crate::error::TaskError::application)
    /// failure. A failure raised with
    /// [`TaskError::non_retryable`](crate::error::TaskError::non_retryable)
    /// is not retried whether or not its type is listed here.
    #[serde(default)]
    pub non_retryable_errors: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            jitter: JitterStrategy::None,
            non_retryable_errors: Vec::new(),
        }
    }
}

impl RetryPolicy {
    /// Create a retry policy with the given limits and no jitter.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::task::RetryPolicy;
    /// # use std::time::Duration;
    /// let policy = RetryPolicy::new(
    ///     5,
    ///     Duration::from_secs(1),
    ///     Duration::from_secs(120),
    ///     2.0,
    /// );
    /// ```
    pub fn new(
        max_attempts: u32,
        initial_interval: Duration,
        max_interval: Duration,
        backoff_coefficient: f64,
    ) -> Self {
        Self {
            max_attempts,
            initial_interval,
            max_interval,
            backoff_coefficient,
            jitter: JitterStrategy::None,
            non_retryable_errors: Vec::new(),
        }
    }

    /// Set the maximum number of attempts, the first included (builder-style).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the wait before the first retry (builder-style).
    pub fn with_initial_interval(mut self, interval: Duration) -> Self {
        self.initial_interval = interval;
        self
    }

    /// Cap the wait between retries (builder-style).
    pub fn with_max_interval(mut self, interval: Duration) -> Self {
        self.max_interval = interval;
        self
    }

    /// Set the multiplier applied to the wait after each retry (builder-style).
    pub fn with_backoff_coefficient(mut self, coefficient: f64) -> Self {
        self.backoff_coefficient = coefficient;
        self
    }

    /// Never retry failures reported under these error types (builder-style).
    pub fn with_non_retryable_errors<I, S>(mut self, error_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.non_retryable_errors = error_types.into_iter().map(Into::into).collect();
        self
    }

    /// Set the jitter strategy (builder-style).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::task::{RetryPolicy, JitterStrategy};
    /// # use std::time::Duration;
    /// let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(1))
    ///     .with_jitter(JitterStrategy::Full);
    /// ```
    pub fn with_jitter(mut self, jitter: JitterStrategy) -> Self {
        self.jitter = jitter;
        self
    }

    /// Create a policy that makes a single attempt and never retries.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::task::RetryPolicy;
    /// let policy = RetryPolicy::no_retry();
    /// assert_eq!(policy.max_attempts, 1);
    /// ```
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            initial_interval: Duration::from_secs(0),
            max_interval: Duration::from_secs(0),
            backoff_coefficient: 1.0,
            jitter: JitterStrategy::None,
            non_retryable_errors: Vec::new(),
        }
    }

    /// Create a policy that doubles the wait after each retry, up to one hour.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::task::RetryPolicy;
    /// # use std::time::Duration;
    /// let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(1));
    /// assert_eq!(policy.max_attempts, 5);
    /// assert_eq!(policy.backoff_coefficient, 2.0);
    /// ```
    pub fn exponential_backoff(max_attempts: u32, initial_interval: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval,
            max_interval: Duration::from_secs(3600), // 1 hour max
            backoff_coefficient: 2.0,
            jitter: JitterStrategy::None,
            non_retryable_errors: Vec::new(),
        }
    }

    /// Return the wait after the failed attempt numbered `attempt` (1-based).
    ///
    /// The backoff interval is `initial_interval * backoff_coefficient^(attempt - 1)`,
    /// capped at `max_interval`, with the jitter strategy applied. Decorrelated
    /// jitter needs the previous wait; use
    /// [`retry_interval_decorrelated`](Self::retry_interval_decorrelated) for it.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::task::RetryPolicy;
    /// # use std::time::Duration;
    /// let policy = RetryPolicy::default();
    /// assert_eq!(policy.retry_interval(1), Duration::from_secs(1));
    /// assert_eq!(policy.retry_interval(2), Duration::from_secs(2));
    /// ```
    pub fn retry_interval(&self, attempt: u32) -> Duration {
        let base = self.base_interval(attempt);
        self.apply_jitter(base, base)
    }

    /// Return the next wait given the previous one.
    ///
    /// With [`JitterStrategy::Decorrelated`] the result is random in
    /// `[initial_interval, prev_interval * 3)`, capped at `max_interval`. With any
    /// other strategy, that strategy's jitter is applied to `prev_interval` itself.
    pub fn retry_interval_decorrelated(&self, prev_interval: Duration) -> Duration {
        match self.jitter {
            JitterStrategy::Decorrelated => {
                let mut rng = rand::thread_rng();
                let min = self.initial_interval.as_secs_f64();
                let max = (prev_interval.as_secs_f64() * 3.0).min(self.max_interval.as_secs_f64());
                let jittered = if max > min {
                    rng.gen_range(min..max)
                } else {
                    min
                };
                Duration::from_secs_f64(jittered)
            }
            _ => {
                // No attempt number is available here, so the previous wait
                // stands in for the backoff interval.
                let base = prev_interval;
                self.apply_jitter(base, base)
            }
        }
    }

    /// Return the capped exponential backoff interval, without jitter.
    fn base_interval(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return self.initial_interval;
        }

        let multiplier = self.backoff_coefficient.powi((attempt - 1) as i32);
        let interval_secs = self.initial_interval.as_secs_f64() * multiplier;
        let capped_secs = interval_secs.min(self.max_interval.as_secs_f64());

        Duration::from_secs_f64(capped_secs)
    }

    /// Apply the jitter strategy to `base`.
    ///
    /// Random ranges use a tiny positive upper bound when `base` is zero, because
    /// an empty range would panic.
    fn apply_jitter(&self, base: Duration, _prev: Duration) -> Duration {
        let base_secs = base.as_secs_f64();
        match self.jitter {
            JitterStrategy::None => base,
            JitterStrategy::Full => {
                let jittered = rand::thread_rng().gen_range(0.0..base_secs.max(f64::MIN_POSITIVE));
                Duration::from_secs_f64(jittered)
            }
            JitterStrategy::Equal => {
                let half = base_secs / 2.0;
                let jittered =
                    half + rand::thread_rng().gen_range(0.0..half.max(f64::MIN_POSITIVE));
                Duration::from_secs_f64(jittered)
            }
            JitterStrategy::Decorrelated => {
                // Without a previous wait, decorrelated jitter falls back to full jitter.
                let jittered = rand::thread_rng().gen_range(0.0..base_secs.max(f64::MIN_POSITIVE));
                Duration::from_secs_f64(jittered)
            }
        }
    }
}

/// Run an async operation, retrying failures according to `policy`.
///
/// The operation is called up to `policy.max_attempts` times, sleeping between
/// attempts as the policy's backoff and jitter dictate. Whether an error is retried
/// is decided only by [`Error::is_retryable`](crate::Error::is_retryable);
/// `policy.non_retryable_errors` is not consulted.
///
/// # Errors
///
/// Returns the first non-retryable error, or the last error once all attempts
/// have failed. If `max_attempts` is 0 the operation is never called and an
/// [`Error::Other`](crate::Error::Other) is returned.
///
/// # Example
///
/// ```rust
/// # use orcher::task::{RetryPolicy, JitterStrategy, retry_with_policy};
/// # use std::time::Duration;
/// # async fn example() -> orcher::Result<String> {
/// let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(1))
///     .with_jitter(JitterStrategy::Full);
///
/// let result = retry_with_policy(&policy, || async {
///     // The fallible operation goes here.
///     Ok("success".to_string())
/// }).await?;
/// # Ok(result)
/// # }
/// ```
pub async fn retry_with_policy<F, Fut, T>(policy: &RetryPolicy, f: F) -> crate::error::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = crate::error::Result<T>>,
{
    let mut last_error = None;
    let mut prev_interval = policy.initial_interval;

    for attempt in 1..=policy.max_attempts {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if !e.is_retryable() || attempt == policy.max_attempts {
                    return Err(e);
                }

                let interval = if policy.jitter == JitterStrategy::Decorrelated {
                    let interval = policy.retry_interval_decorrelated(prev_interval);
                    prev_interval = interval;
                    interval
                } else {
                    policy.retry_interval(attempt)
                };

                tracing::debug!(
                    attempt = attempt,
                    max_attempts = policy.max_attempts,
                    next_interval_ms = interval.as_millis() as u64,
                    error = %e,
                    "Retrying after error"
                );

                last_error = Some(e);
                tokio::time::sleep(interval).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        crate::Error::Other("Retry policy exhausted without any attempts".to_string())
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_interval, Duration::from_secs(1));
        assert_eq!(policy.max_interval, Duration::from_secs(60));
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_retry_policy_no_retry() {
        let policy = RetryPolicy::no_retry();
        assert_eq!(policy.max_attempts, 1);
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(1));
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_interval, Duration::from_secs(1));
        assert_eq!(policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn test_retry_interval_calculation() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(10),
            backoff_coefficient: 2.0,
            jitter: JitterStrategy::None,
            non_retryable_errors: vec![],
        };

        // 1st attempt: 1s
        assert_eq!(policy.retry_interval(1), Duration::from_secs(1));

        // 2nd attempt: 1s * 2 = 2s
        assert_eq!(policy.retry_interval(2), Duration::from_secs(2));

        // 3rd attempt: 1s * 4 = 4s
        assert_eq!(policy.retry_interval(3), Duration::from_secs(4));

        // 4th attempt: 1s * 8 = 8s
        assert_eq!(policy.retry_interval(4), Duration::from_secs(8));

        // 5th attempt: 1s * 16 = 16s, but capped at max_interval (10s)
        assert_eq!(policy.retry_interval(5), Duration::from_secs(10));
    }

    #[test]
    fn test_retry_policy_serialization() {
        let policy = RetryPolicy::default();

        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("max_attempts"));

        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_attempts, policy.max_attempts);
    }

    #[test]
    fn test_retry_policy_new() {
        let policy = RetryPolicy::new(10, Duration::from_secs(2), Duration::from_secs(300), 1.5);

        assert_eq!(policy.max_attempts, 10);
        assert_eq!(policy.initial_interval, Duration::from_secs(2));
        assert_eq!(policy.max_interval, Duration::from_secs(300));
        assert_eq!(policy.backoff_coefficient, 1.5);
        assert_eq!(policy.jitter, JitterStrategy::None);
    }

    #[test]
    fn test_with_jitter_builder() {
        let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(1))
            .with_jitter(JitterStrategy::Full);

        assert_eq!(policy.jitter, JitterStrategy::Full);
        assert_eq!(policy.max_attempts, 5);
    }

    #[test]
    fn test_full_jitter_bounded() {
        let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(10))
            .with_jitter(JitterStrategy::Full);

        for attempt in 1..=5 {
            let base = policy.base_interval(attempt);
            let interval = policy.retry_interval(attempt);
            assert!(interval <= base, "Full jitter should be <= base interval");
        }
    }

    #[test]
    fn test_equal_jitter_bounded() {
        let policy = RetryPolicy::exponential_backoff(5, Duration::from_secs(10))
            .with_jitter(JitterStrategy::Equal);

        for attempt in 1..=5 {
            let base = policy.base_interval(attempt);
            let interval = policy.retry_interval(attempt);
            let half = base / 2;
            assert!(interval >= half, "Equal jitter should be >= base/2");
            assert!(interval <= base, "Equal jitter should be <= base");
        }
    }

    #[test]
    fn test_jitter_strategy_default() {
        assert_eq!(JitterStrategy::default(), JitterStrategy::None);
    }

    #[test]
    fn test_jitter_serialization() {
        let policy = RetryPolicy::default().with_jitter(JitterStrategy::Full);
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("Full"));

        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.jitter, JitterStrategy::Full);
    }

    #[tokio::test]
    async fn test_retry_with_policy_success() {
        let policy = RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10), 2.0);
        let result = retry_with_policy(&policy, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_with_policy_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = std::sync::Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let policy = RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10), 2.0);
        let result = retry_with_policy(&policy, || {
            let attempts = attempts_clone.clone();
            async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    // Use a retryable error variant (connection failure)
                    Err(crate::Error::Client(
                        crate::error::ClientError::ConnectionFailed {
                            url: "http://test".to_string(),
                            reason: "transient".to_string(),
                        },
                    ))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_policy_permanent_error_no_retry() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = std::sync::Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let policy = RetryPolicy::new(5, Duration::from_millis(1), Duration::from_millis(10), 2.0);
        let result: crate::error::Result<i32> = retry_with_policy(&policy, || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(crate::Error::Configuration("permanent".to_string()))
            }
        })
        .await;

        assert!(result.is_err());
        // Permanent error should not be retried
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
