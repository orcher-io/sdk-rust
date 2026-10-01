//! In-memory state storage for testing actors without a server.
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk::actor::state::MockStateBackend;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let backend = MockStateBackend::new();
//!
//! backend.set("actor:Counter:key-1:count", vec![1, 2, 3]).await;
//!
//! let value = backend.get("actor:Counter:key-1:count").await;
//! assert_eq!(value, Some(vec![1, 2, 3]));
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory state storage for tests.
///
/// Stores raw bytes under full storage keys (`actor:{actor_name}:{key}:{state_key}`),
/// with no persistence and no versioning. Clones share the same storage and counters.
#[derive(Clone, Debug)]
pub struct MockStateBackend {
    storage: Arc<RwLock<HashMap<String, Vec<u8>>>>,

    /// Calls to get, set, and delete, for assertions in tests.
    get_count: Arc<RwLock<usize>>,
    set_count: Arc<RwLock<usize>>,
    delete_count: Arc<RwLock<usize>>,
}

impl MockStateBackend {
    /// Creates empty storage with zeroed counters.
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            get_count: Arc::new(RwLock::new(0)),
            set_count: Arc::new(RwLock::new(0)),
            delete_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Returns the value stored under `key`, if any. Counts as a get.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        {
            let mut count = self.get_count.write().await;
            *count += 1;
        }

        let storage = self.storage.read().await;
        storage.get(key).cloned()
    }

    /// Stores `value` under `key`, replacing any previous value. Counts as a set.
    pub async fn set(&self, key: &str, value: Vec<u8>) {
        {
            let mut count = self.set_count.write().await;
            *count += 1;
        }

        let mut storage = self.storage.write().await;
        storage.insert(key.to_string(), value);
    }

    /// Removes `key` and returns whether it existed. Counts as a delete.
    pub async fn delete(&self, key: &str) -> bool {
        {
            let mut count = self.delete_count.write().await;
            *count += 1;
        }

        let mut storage = self.storage.write().await;
        storage.remove(key).is_some()
    }

    /// Returns every stored key that starts with `prefix`, in no particular order.
    pub async fn list_keys(&self, prefix: &str) -> Vec<String> {
        let storage = self.storage.read().await;
        storage
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Removes all data and resets the operation counters.
    pub async fn clear(&self) {
        let mut storage = self.storage.write().await;
        storage.clear();

        *self.get_count.write().await = 0;
        *self.set_count.write().await = 0;
        *self.delete_count.write().await = 0;
    }

    /// Returns the number of stored keys.
    pub async fn len(&self) -> usize {
        let storage = self.storage.read().await;
        storage.len()
    }

    /// Returns whether nothing is stored.
    pub async fn is_empty(&self) -> bool {
        let storage = self.storage.read().await;
        storage.is_empty()
    }

    /// Returns how many gets, sets, and deletes have been made since creation or
    /// the last [`clear`](Self::clear).
    pub async fn get_operation_counts(&self) -> OperationCounts {
        OperationCounts {
            gets: *self.get_count.read().await,
            sets: *self.set_count.read().await,
            deletes: *self.delete_count.read().await,
        }
    }

    /// Returns whether `key` is stored. Not counted as a get.
    pub async fn exists(&self, key: &str) -> bool {
        let storage = self.storage.read().await;
        storage.contains_key(key)
    }

    /// Returns every stored key, in no particular order.
    pub async fn keys(&self) -> Vec<String> {
        let storage = self.storage.read().await;
        storage.keys().cloned().collect()
    }

    /// Returns a copy of all stored data.
    pub async fn snapshot(&self) -> HashMap<String, Vec<u8>> {
        let storage = self.storage.read().await;
        storage.clone()
    }
}

impl Default for MockStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

/// Operation counts recorded by [`MockStateBackend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationCounts {
    /// Number of `get` calls.
    pub gets: usize,
    /// Number of `set` calls.
    pub sets: usize,
    /// Number of `delete` calls.
    pub deletes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let backend = MockStateBackend::new();

        assert!(backend.is_empty().await);
        assert_eq!(backend.len().await, 0);

        backend.set("key1", vec![1, 2, 3]).await;
        assert_eq!(backend.len().await, 1);

        let value = backend.get("key1").await;
        assert_eq!(value, Some(vec![1, 2, 3]));

        assert!(backend.exists("key1").await);
        assert!(!backend.exists("key2").await);
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = MockStateBackend::new();

        backend.set("key1", vec![1, 2, 3]).await;
        assert!(backend.exists("key1").await);

        let deleted = backend.delete("key1").await;
        assert!(deleted);
        assert!(!backend.exists("key1").await);

        // Deleting a missing key reports false.
        let deleted = backend.delete("key1").await;
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_list_keys() {
        let backend = MockStateBackend::new();

        backend.set("actor:Counter:key1:state", vec![1]).await;
        backend.set("actor:Counter:key2:state", vec![2]).await;
        backend.set("actor:Cart:key1:state", vec![3]).await;

        let counter_keys = backend.list_keys("actor:Counter:").await;
        assert_eq!(counter_keys.len(), 2);

        let cart_keys = backend.list_keys("actor:Cart:").await;
        assert_eq!(cart_keys.len(), 1);

        // An empty prefix matches every key.
        let all_keys = backend.list_keys("").await;
        assert_eq!(all_keys.len(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let backend = MockStateBackend::new();

        backend.set("key1", vec![1]).await;
        backend.set("key2", vec![2]).await;
        assert_eq!(backend.len().await, 2);

        backend.clear().await;
        assert_eq!(backend.len().await, 0);
        assert!(backend.is_empty().await);
    }

    #[tokio::test]
    async fn test_operation_counts() {
        let backend = MockStateBackend::new();

        backend.set("key1", vec![1]).await;
        backend.get("key1").await;
        backend.get("key1").await;
        backend.delete("key1").await;

        let counts = backend.get_operation_counts().await;
        assert_eq!(counts.gets, 2);
        assert_eq!(counts.sets, 1);
        assert_eq!(counts.deletes, 1);
    }

    #[tokio::test]
    async fn test_overwrite() {
        let backend = MockStateBackend::new();

        backend.set("key1", vec![1, 2, 3]).await;
        backend.set("key1", vec![4, 5, 6]).await;

        let value = backend.get("key1").await;
        assert_eq!(value, Some(vec![4, 5, 6]));
        assert_eq!(backend.len().await, 1);
    }

    #[tokio::test]
    async fn test_clone() {
        let backend1 = MockStateBackend::new();
        backend1.set("key1", vec![1, 2, 3]).await;

        // Clones share storage, in both directions.
        let backend2 = backend1.clone();
        let value = backend2.get("key1").await;
        assert_eq!(value, Some(vec![1, 2, 3]));

        backend2.set("key2", vec![4, 5, 6]).await;
        assert!(backend1.exists("key2").await);
    }

    #[tokio::test]
    async fn test_snapshot() {
        let backend = MockStateBackend::new();

        backend.set("key1", vec![1]).await;
        backend.set("key2", vec![2]).await;

        let snapshot = backend.snapshot().await;
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get("key1"), Some(&vec![1]));
        assert_eq!(snapshot.get("key2"), Some(&vec![2]));
    }
}
