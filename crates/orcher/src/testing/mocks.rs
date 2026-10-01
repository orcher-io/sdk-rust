//! Task mocks, which stand in for task implementations in tests.

use crate::error::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Task mocks keyed by task type.
pub struct MockRegistry {
    mocks: HashMap<String, TaskMock>,
}

impl MockRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            mocks: HashMap::new(),
        }
    }

    /// Register a mock for a task type, replacing any earlier one.
    pub fn register(&mut self, task_type: String, mock: TaskMock) {
        self.mocks.insert(task_type, mock);
    }

    /// The mock for a task type, if any.
    pub fn get(&self, task_type: &str) -> Option<&TaskMock> {
        self.mocks.get(task_type)
    }

    /// Whether a task type has a mock.
    pub fn has_mock(&self, task_type: &str) -> bool {
        self.mocks.contains_key(task_type)
    }

    /// Run the mock for a task type with `input`.
    ///
    /// # Errors
    ///
    /// Returns an error if the task type has no mock, or the mock fails or its
    /// result does not deserialize as `O`.
    pub fn execute_mock<I, O>(&mut self, task_type: &str, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let mock = self.mocks.get_mut(task_type).ok_or_else(|| {
            Error::Workflow(crate::error::WorkflowError::StateError(format!(
                "No mock registered for task '{}'",
                task_type
            )))
        })?;

        mock.execute(input)
    }
}

impl Default for MockRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// How a mocked task responds when called. Results are stored as JSON bytes.
pub enum TaskMock {
    /// Return the same value on every call
    Fixed {
        /// JSON result to return
        result: Vec<u8>,
    },

    /// Return the next value on each call, then fail once the values run out
    Sequence {
        /// JSON results, in call order
        results: Vec<Vec<u8>>,
        /// Index of the result the next call returns
        index: usize,
    },

    /// Compute the result with a function
    Function {
        /// Takes the JSON input and returns the JSON result
        func: Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync>,
    },

    /// Fail on every call
    Error {
        /// Message of the returned error
        message: String,
    },
}

impl TaskMock {
    /// Run the mock with `input` and deserialize its JSON result as `O`.
    ///
    /// # Errors
    ///
    /// Returns an error if the mock is an [`Error`](TaskMock::Error) mock, a sequence is
    /// exhausted, the function fails, or (de)serialization fails.
    pub fn execute<I, O>(&mut self, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| Error::Serialization(format!("Failed to serialize input: {}", e)))?;

        let result_bytes = match self {
            TaskMock::Fixed { result } => result.clone(),

            TaskMock::Sequence { results, index } => {
                if *index >= results.len() {
                    return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                        format!("Mock sequence exhausted (called {} times)", index),
                    )));
                }
                let result = results[*index].clone();
                *index += 1;
                result
            }

            TaskMock::Function { func } => func(input_bytes)?,

            TaskMock::Error { message } => {
                return Err(Error::Workflow(crate::error::WorkflowError::StateError(
                    message.clone(),
                )));
            }
        };

        let result: O = serde_json::from_slice(&result_bytes)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize result: {}", e)))?;

        Ok(result)
    }
}

/// Registers a mock for one task type. Each method registers the mock and consumes
/// the builder.
pub struct MockTaskBuilder {
    task_type: String,

    registry: Arc<RwLock<MockRegistry>>,
}

impl MockTaskBuilder {
    /// Create a builder that registers into `registry` under `task_type`.
    pub fn new(task_type: String, registry: Arc<RwLock<MockRegistry>>) -> Self {
        Self {
            task_type,
            registry,
        }
    }

    /// Return `result` on every call.
    ///
    /// # Panics
    ///
    /// Panics if `result` does not serialize to JSON.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use serde::Serialize;
    /// #[derive(Serialize)]
    /// struct ValidationResult {
    ///     valid: bool,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.mock_task("validate_order")
    ///     .returns(ValidationResult { valid: true });
    /// ```
    pub fn returns<T: Serialize>(self, result: T) {
        let result_bytes = serde_json::to_vec(&result).expect("Failed to serialize mock result");

        let mock = TaskMock::Fixed {
            result: result_bytes,
        };

        let mut registry = self.registry.write().unwrap();
        registry.register(self.task_type, mock);
    }

    /// Return the values in order, one per call.
    ///
    /// Once the values run out, further calls fail. Pass the success values directly;
    /// to mock a failure, use [`fails_with`](Self::fails_with).
    ///
    /// # Panics
    ///
    /// Panics if a value does not serialize to JSON.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// # use serde::Serialize;
    /// #[derive(Serialize)]
    /// struct Page {
    ///     number: u32,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.mock_task("fetch_page")
    ///     .returns_seq(vec![
    ///         Page { number: 1 },
    ///         Page { number: 2 },
    ///         Page { number: 3 },
    ///     ]);
    /// ```
    pub fn returns_seq<T: Serialize>(self, results: Vec<T>) {
        let result_bytes: Vec<Vec<u8>> = results
            .into_iter()
            .map(|r| serde_json::to_vec(&r).expect("Failed to serialize mock result"))
            .collect();

        let mock = TaskMock::Sequence {
            results: result_bytes,
            index: 0,
        };

        let mut registry = self.registry.write().unwrap();
        registry.register(self.task_type, mock);
    }

    /// Compute each result with `func`.
    ///
    /// The function receives the JSON input bytes and returns the JSON result bytes.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// use orcher::Error;
    /// # use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Item {
    ///     price: f64,
    /// }
    ///
    /// let mut env = TestEnv::new();
    /// env.mock_task("calculate_total")
    ///     .with_fn(|input_bytes| {
    ///         let items: Vec<Item> = serde_json::from_slice(&input_bytes)
    ///             .map_err(|e| Error::Serialization(e.to_string()))?;
    ///         let total: f64 = items.iter().map(|i| i.price).sum();
    ///         serde_json::to_vec(&total).map_err(|e| Error::Serialization(e.to_string()))
    ///     });
    /// ```
    pub fn with_fn<F>(self, func: F)
    where
        F: Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        let mock = TaskMock::Function {
            func: Arc::new(func),
        };

        let mut registry = self.registry.write().unwrap();
        registry.register(self.task_type, mock);
    }

    /// Fail every call with `message`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use orcher::testing::TestEnv;
    /// let mut env = TestEnv::new();
    /// env.mock_task("flaky_service")
    ///     .fails_with("Service unavailable");
    /// ```
    pub fn fails_with(self, message: impl Into<String>) {
        let mock = TaskMock::Error {
            message: message.into(),
        };

        let mut registry = self.registry.write().unwrap();
        registry.register(self.task_type, mock);
    }
}

/// Alias of [`TaskMock`].
pub type MockTask = TaskMock;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_registry_creation() {
        let registry = MockRegistry::new();
        assert!(!registry.has_mock("test_task"));
    }

    #[test]
    fn test_fixed_mock() {
        let mut registry = MockRegistry::new();

        let result = 42i32;
        let result_bytes = serde_json::to_vec(&result).unwrap();

        registry.register(
            "test_task".to_string(),
            TaskMock::Fixed {
                result: result_bytes,
            },
        );

        assert!(registry.has_mock("test_task"));

        let output: i32 = registry.execute_mock("test_task", ()).unwrap();
        assert_eq!(output, 42);
    }

    #[test]
    fn test_sequence_mock() {
        let mut registry = MockRegistry::new();

        let results = vec![1i32, 2, 3];
        let result_bytes: Vec<Vec<u8>> = results
            .into_iter()
            .map(|r| serde_json::to_vec(&r).unwrap())
            .collect();

        registry.register(
            "test_task".to_string(),
            TaskMock::Sequence {
                results: result_bytes,
                index: 0,
            },
        );

        let output1: i32 = registry.execute_mock("test_task", ()).unwrap();
        assert_eq!(output1, 1);

        let output2: i32 = registry.execute_mock("test_task", ()).unwrap();
        assert_eq!(output2, 2);

        let output3: i32 = registry.execute_mock("test_task", ()).unwrap();
        assert_eq!(output3, 3);

        // The sequence is exhausted.
        let result: Result<i32> = registry.execute_mock("test_task", ());
        assert!(result.is_err());
    }

    #[test]
    fn test_function_mock() {
        let mut registry = MockRegistry::new();

        registry.register(
            "double".to_string(),
            TaskMock::Function {
                func: Arc::new(|input_bytes| {
                    let value: i32 = serde_json::from_slice(&input_bytes)?;
                    let result = value * 2;
                    Ok(serde_json::to_vec(&result)?)
                }),
            },
        );

        let output: i32 = registry.execute_mock("double", 21).unwrap();
        assert_eq!(output, 42);
    }

    #[test]
    fn test_error_mock() {
        let mut registry = MockRegistry::new();

        registry.register(
            "failing_task".to_string(),
            TaskMock::Error {
                message: "Task failed".to_string(),
            },
        );

        let result: Result<i32> = registry.execute_mock("failing_task", ());
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_not_registered() {
        let mut registry = MockRegistry::new();
        let result: Result<i32> = registry.execute_mock("unknown_task", ());
        assert!(result.is_err());
    }
}
