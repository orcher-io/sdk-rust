//! Point-in-time copies of workflow state, and diffs between them.

use crate::error::{Error, Result};
use crate::workflow::WorkflowContext;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// A copy of workflow state at a point in time.
///
/// Values are stored as JSON bytes, so two snapshots compare equal on a key
/// when the serialized values are byte-identical.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    /// ID of the workflow the snapshot belongs to.
    pub workflow_id: String,

    /// Wall-clock time at which the snapshot was taken.
    pub timestamp: SystemTime,

    /// State values keyed by name, each serialized as JSON.
    state: HashMap<String, Vec<u8>>,

    label: Option<String>,
}

impl StateSnapshot {
    /// Capture a snapshot from a workflow context.
    ///
    /// The snapshot records the workflow ID and the capture time. It does not
    /// read the context's state, so it starts empty; add values with
    /// [`set`](Self::set).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::debugging::StateSnapshot;
    /// # use orcher::workflow::WorkflowContext;
    /// # fn example(ctx: &WorkflowContext) {
    /// let snapshot = StateSnapshot::capture(ctx);
    /// println!("Captured {} state keys", snapshot.keys().len());
    /// # }
    /// ```
    pub fn capture(ctx: &WorkflowContext) -> Self {
        Self {
            workflow_id: ctx.workflow_id().to_string(),
            timestamp: SystemTime::now(),
            state: HashMap::new(),
            label: None,
        }
    }

    /// Capture a snapshot from a workflow context and label it.
    ///
    /// Behaves like [`capture`](Self::capture).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::debugging::StateSnapshot;
    /// # use orcher::workflow::WorkflowContext;
    /// # fn example(ctx: &WorkflowContext) {
    /// let snapshot = StateSnapshot::capture_with_label(ctx, "after_payment");
    /// # }
    /// ```
    pub fn capture_with_label(ctx: &WorkflowContext, label: impl Into<String>) -> Self {
        let mut snapshot = Self::capture(ctx);
        snapshot.label = Some(label.into());
        snapshot
    }

    /// Create an empty snapshot for the given workflow ID.
    pub fn new(workflow_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            timestamp: SystemTime::now(),
            state: HashMap::new(),
            label: None,
        }
    }

    /// Store a value under `key`, replacing any existing value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if the value cannot be serialized to JSON.
    pub fn set<T: Serialize>(&mut self, key: impl Into<String>, value: &T) -> Result<()> {
        let key = key.into();
        let value_bytes = serde_json::to_vec(value)
            .map_err(|e| Error::Serialization(format!("Failed to serialize value: {}", e)))?;
        self.state.insert(key, value_bytes);
        Ok(())
    }

    /// Read the value stored under `key`, or `None` if the key is absent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if the stored value does not deserialize as `T`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::debugging::StateSnapshot;
    /// # fn example() -> orcher::Result<()> {
    /// # let snapshot = StateSnapshot::new("wf-123");
    /// let counter: Option<i32> = snapshot.get("counter")?;
    /// if let Some(value) = counter {
    ///     println!("Counter: {}", value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.state.get(key) {
            Some(value_bytes) => {
                let value: T = serde_json::from_slice(value_bytes).map_err(|e| {
                    Error::Serialization(format!("Failed to deserialize value: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Return whether the snapshot holds a value for `key`.
    pub fn contains(&self, key: &str) -> bool {
        self.state.contains_key(key)
    }

    /// Return all keys, in no particular order.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::debugging::StateSnapshot;
    /// # fn example(snapshot: &StateSnapshot) {
    /// let keys = snapshot.keys();
    /// println!("State keys: {:?}", keys);
    /// # }
    /// ```
    pub fn keys(&self) -> Vec<String> {
        self.state.keys().cloned().collect()
    }

    /// Return the number of keys.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Return whether the snapshot holds no values.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Return the snapshot's label, if it has one.
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Compare this snapshot with a later one.
    ///
    /// `self` is the "from" side: keys only in `other` are added, keys only in
    /// `self` are removed, and keys in both with different values are changed.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::debugging::StateSnapshot;
    /// # fn example() {
    /// # let snapshot1 = StateSnapshot::new("wf-123");
    /// # let snapshot2 = StateSnapshot::new("wf-123");
    /// let diff = snapshot1.diff(&snapshot2);
    /// println!("Changed keys: {:?}", diff.changed_keys());
    /// println!("Added keys: {:?}", diff.added_keys());
    /// println!("Removed keys: {:?}", diff.removed_keys());
    /// # }
    /// ```
    pub fn diff(&self, other: &StateSnapshot) -> StateDiff {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut changed = Vec::new();

        for key in other.keys() {
            if !self.contains(&key) {
                added.push(key.clone());
            } else if self.state.get(&key) != other.state.get(&key) {
                changed.push(key.clone());
            }
        }

        for key in self.keys() {
            if !other.contains(&key) {
                removed.push(key.clone());
            }
        }

        StateDiff {
            from_label: self.label.clone(),
            to_label: other.label.clone(),
            added_keys: added,
            removed_keys: removed,
            changed_keys: changed,
        }
    }

    /// Set the label, replacing any existing one (builder-style).
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Return the underlying map of keys to JSON-encoded values.
    pub fn raw_state(&self) -> &HashMap<String, Vec<u8>> {
        &self.state
    }
}

/// Differences between two state snapshots, produced by [`StateSnapshot::diff`].
#[derive(Debug, Clone)]
pub struct StateDiff {
    /// Label of the earlier snapshot.
    pub from_label: Option<String>,

    /// Label of the later snapshot.
    pub to_label: Option<String>,

    /// Keys present only in the later snapshot.
    pub added_keys: Vec<String>,

    /// Keys present only in the earlier snapshot.
    pub removed_keys: Vec<String>,

    /// Keys present in both snapshots with different values.
    pub changed_keys: Vec<String>,
}

impl StateDiff {
    /// Return whether any key was added, removed or changed.
    pub fn has_changes(&self) -> bool {
        !self.added_keys.is_empty()
            || !self.removed_keys.is_empty()
            || !self.changed_keys.is_empty()
    }

    /// Return added, removed and changed keys, in that order.
    pub fn all_changed_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        keys.extend(self.added_keys.iter().cloned());
        keys.extend(self.removed_keys.iter().cloned());
        keys.extend(self.changed_keys.iter().cloned());
        keys
    }

    /// Return the total number of added, removed and changed keys.
    pub fn change_count(&self) -> usize {
        self.added_keys.len() + self.removed_keys.len() + self.changed_keys.len()
    }

    /// Return a short summary such as `"1 added, 2 changed"`, or `"No changes"`.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.added_keys.is_empty() {
            parts.push(format!("{} added", self.added_keys.len()));
        }
        if !self.removed_keys.is_empty() {
            parts.push(format!("{} removed", self.removed_keys.len()));
        }
        if !self.changed_keys.is_empty() {
            parts.push(format!("{} changed", self.changed_keys.len()));
        }

        if parts.is_empty() {
            "No changes".to_string()
        } else {
            parts.join(", ")
        }
    }

    /// Return the keys present only in the later snapshot.
    pub fn added_keys(&self) -> &[String] {
        &self.added_keys
    }

    /// Return the keys present only in the earlier snapshot.
    pub fn removed_keys(&self) -> &[String] {
        &self.removed_keys
    }

    /// Return the keys whose values differ between the snapshots.
    pub fn changed_keys(&self) -> &[String] {
        &self.changed_keys
    }
}

/// An ordered series of snapshots of one workflow's state.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Not re-exported from `debugging`; exercised by the tests below.
pub struct StateHistory {
    workflow_id: String,

    /// Snapshots in the order they were added.
    snapshots: Vec<StateSnapshot>,
}

#[allow(dead_code)] // See the note on the struct.
impl StateHistory {
    /// Create an empty history for the given workflow ID.
    pub fn new(workflow_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            snapshots: Vec::new(),
        }
    }

    /// Append a snapshot.
    pub fn add(&mut self, snapshot: StateSnapshot) {
        self.snapshots.push(snapshot);
    }

    /// Capture a snapshot from a context, optionally labeled, and append it.
    pub fn capture(&mut self, ctx: &WorkflowContext, label: Option<String>) {
        let snapshot = if let Some(label) = label {
            StateSnapshot::capture_with_label(ctx, label)
        } else {
            StateSnapshot::capture(ctx)
        };
        self.add(snapshot);
    }

    /// Return all snapshots in the order they were added.
    pub fn snapshots(&self) -> &[StateSnapshot] {
        &self.snapshots
    }

    /// Return the most recently added snapshot.
    pub fn latest(&self) -> Option<&StateSnapshot> {
        self.snapshots.last()
    }

    /// Return the first snapshot added.
    pub fn first(&self) -> Option<&StateSnapshot> {
        self.snapshots.first()
    }

    /// Return the number of snapshots.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Return whether the history holds no snapshots.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// Return the first snapshot with the given label.
    pub fn get_by_label(&self, label: &str) -> Option<&StateSnapshot> {
        self.snapshots
            .iter()
            .find(|s| s.label.as_deref() == Some(label))
    }

    /// Diff the snapshots at two indexes, or return `None` if either is out of range.
    pub fn diff_between(&self, from_idx: usize, to_idx: usize) -> Option<StateDiff> {
        if from_idx >= self.snapshots.len() || to_idx >= self.snapshots.len() {
            return None;
        }
        Some(self.snapshots[from_idx].diff(&self.snapshots[to_idx]))
    }

    /// Diff the first snapshot against the latest, or return `None` if there are
    /// fewer than two.
    pub fn overall_diff(&self) -> Option<StateDiff> {
        if self.snapshots.len() < 2 {
            return None;
        }
        let first_idx = 0;
        let last_idx = self.snapshots.len() - 1;
        self.diff_between(first_idx, last_idx)
    }

    /// Remove all snapshots.
    pub fn clear(&mut self) {
        self.snapshots.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_snapshot_creation() {
        let snapshot = StateSnapshot::new("wf-123");
        assert_eq!(snapshot.workflow_id, "wf-123");
        assert!(snapshot.is_empty());
        assert!(snapshot.label.is_none());
    }

    #[test]
    fn test_state_snapshot_set_get() {
        let mut snapshot = StateSnapshot::new("wf-123");
        snapshot.set("counter", &42i32).unwrap();
        snapshot.set("name", &"test".to_string()).unwrap();

        let counter: Option<i32> = snapshot.get("counter").unwrap();
        assert_eq!(counter, Some(42));

        let name: Option<String> = snapshot.get("name").unwrap();
        assert_eq!(name, Some("test".to_string()));

        let missing: Option<String> = snapshot.get("missing").unwrap();
        assert_eq!(missing, None);
    }

    #[test]
    fn test_state_snapshot_keys() {
        let mut snapshot = StateSnapshot::new("wf-123");
        snapshot.set("key1", &1).unwrap();
        snapshot.set("key2", &2).unwrap();

        let keys = snapshot.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[test]
    fn test_state_snapshot_contains() {
        let mut snapshot = StateSnapshot::new("wf-123");
        snapshot.set("exists", &true).unwrap();

        assert!(snapshot.contains("exists"));
        assert!(!snapshot.contains("missing"));
    }

    #[test]
    fn test_state_snapshot_with_label() {
        let snapshot = StateSnapshot::new("wf-123").with_label("after_payment");
        assert_eq!(snapshot.label(), Some("after_payment"));
    }

    #[test]
    fn test_state_diff_no_changes() {
        let mut s1 = StateSnapshot::new("wf-123");
        s1.set("key", &42).unwrap();

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("key", &42).unwrap();

        let diff = s1.diff(&s2);
        assert!(!diff.has_changes());
        assert_eq!(diff.change_count(), 0);
    }

    #[test]
    fn test_state_diff_added_keys() {
        let s1 = StateSnapshot::new("wf-123");

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("new_key", &1).unwrap();

        let diff = s1.diff(&s2);
        assert!(diff.has_changes());
        assert_eq!(diff.added_keys.len(), 1);
        assert!(diff.added_keys.contains(&"new_key".to_string()));
    }

    #[test]
    fn test_state_diff_removed_keys() {
        let mut s1 = StateSnapshot::new("wf-123");
        s1.set("old_key", &1).unwrap();

        let s2 = StateSnapshot::new("wf-123");

        let diff = s1.diff(&s2);
        assert!(diff.has_changes());
        assert_eq!(diff.removed_keys.len(), 1);
        assert!(diff.removed_keys.contains(&"old_key".to_string()));
    }

    #[test]
    fn test_state_diff_changed_keys() {
        let mut s1 = StateSnapshot::new("wf-123");
        s1.set("key", &1).unwrap();

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("key", &2).unwrap();

        let diff = s1.diff(&s2);
        assert!(diff.has_changes());
        assert_eq!(diff.changed_keys.len(), 1);
        assert!(diff.changed_keys.contains(&"key".to_string()));
    }

    #[test]
    fn test_state_diff_summary() {
        let mut s1 = StateSnapshot::new("wf-123");
        s1.set("old", &1).unwrap();
        s1.set("changed", &1).unwrap();

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("new", &2).unwrap();
        s2.set("changed", &2).unwrap();

        let diff = s1.diff(&s2);
        let summary = diff.summary();
        assert!(summary.contains("added"));
        assert!(summary.contains("removed"));
        assert!(summary.contains("changed"));
    }

    #[test]
    fn test_state_history_creation() {
        let history = StateHistory::new("wf-123");
        assert_eq!(history.workflow_id, "wf-123");
        assert!(history.is_empty());
    }

    #[test]
    fn test_state_history_add() {
        let mut history = StateHistory::new("wf-123");

        let snapshot1 = StateSnapshot::new("wf-123").with_label("snapshot1");
        let snapshot2 = StateSnapshot::new("wf-123").with_label("snapshot2");

        history.add(snapshot1);
        history.add(snapshot2);

        assert_eq!(history.len(), 2);
        assert_eq!(history.first().unwrap().label(), Some("snapshot1"));
        assert_eq!(history.latest().unwrap().label(), Some("snapshot2"));
    }

    #[test]
    fn test_state_history_get_by_label() {
        let mut history = StateHistory::new("wf-123");

        let snapshot = StateSnapshot::new("wf-123").with_label("test_label");
        history.add(snapshot);

        let found = history.get_by_label("test_label");
        assert!(found.is_some());
        assert_eq!(found.unwrap().label(), Some("test_label"));

        let not_found = history.get_by_label("missing");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_state_history_diff_between() {
        let mut history = StateHistory::new("wf-123");

        let mut s1 = StateSnapshot::new("wf-123");
        s1.set("key", &1).unwrap();
        history.add(s1);

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("key", &2).unwrap();
        history.add(s2);

        let diff = history.diff_between(0, 1).unwrap();
        assert!(diff.has_changes());
        assert_eq!(diff.changed_keys.len(), 1);
    }

    #[test]
    fn test_state_history_overall_diff() {
        let mut history = StateHistory::new("wf-123");

        let s1 = StateSnapshot::new("wf-123");
        history.add(s1);

        let mut s2 = StateSnapshot::new("wf-123");
        s2.set("new_key", &1).unwrap();
        history.add(s2);

        let diff = history.overall_diff().unwrap();
        assert!(diff.has_changes());
        assert_eq!(diff.added_keys.len(), 1);
    }

    #[test]
    fn test_state_history_clear() {
        let mut history = StateHistory::new("wf-123");
        history.add(StateSnapshot::new("wf-123"));
        history.add(StateSnapshot::new("wf-123"));

        assert_eq!(history.len(), 2);

        history.clear();

        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }
}
