//! Task references: names of tasks that the compiler checks.
//!
//! A task named by a string is only checked at run time, so a typo or a stale
//! name after a rename fails when the workflow runs. The `#[task]` macro
//! generates a reference for each task, so a misspelled or stale name is a
//! compile error:
//!
//! ```rust
//! use orcher_sdk::prelude::*;
//!
//! #[derive(Serialize, Deserialize)]
//! pub struct Email {
//!     to: String,
//! }
//!
//! #[task(timeout = 30)]
//! pub async fn send_email(_ctx: TaskContext, email: Email) -> Result<String> {
//!     Ok(format!("sent to {}", email.to))
//! }
//!
//! // The macro also generates a constant with the task's name:
//! // pub const send_email: send_email_task_ref = send_email_task_ref;
//!
//! #[workflow]
//! async fn order_workflow(ctx: WorkflowContext, email: Email) -> Result<String> {
//!     let receipt: String = ctx.execute_task(send_email, email).await?;
//!     //                                     ^^^^^^^^^^
//!     //                                     Checked at compile time.
//!
//!     // Misspelled; this would compile and fail only when the workflow runs:
//!     // ctx.execute_task("send_emial", email).await?;
//!     Ok(receipt)
//! }
//! ```
//!
//! A reference is a zero-sized value that resolves to a `&'static str`, so it
//! costs nothing at run time.
//!
//! ## Generated code
//!
//! For a task function, `#[task]` generates code of this shape:
//!
//! ```text
//! // Written:
//! #[task]
//! pub async fn process_payment(ctx: TaskContext, amount: i64) -> Result<Receipt> {
//!     // ...
//! }
//!
//! // Generated (plus a payload-level handler and its registration):
//! pub async fn process_payment_impl(ctx: TaskContext, amount: i64) -> Result<Receipt> {
//!     // ...
//! }
//!
//! #[derive(Debug, Clone, Copy)]
//! #[allow(non_camel_case_types)]
//! pub struct process_payment_task_ref;
//!
//! impl ::orcher_sdk::task::TaskReference for process_payment_task_ref {
//!     fn task_name(&self) -> &'static str {
//!         "process_payment"
//!     }
//! }
//!
//! // The function was renamed, so the task's own name is free for the reference.
//! #[allow(non_upper_case_globals)]
//! pub const process_payment: process_payment_task_ref = process_payment_task_ref;
//! ```

use std::fmt;

/// A compile-time checked reference to a task.
///
/// The `#[task]` and `#[tasks]` macros implement this trait on generated
/// zero-sized types. The trait is not sealed, so it can be implemented by hand,
/// but then nothing checks that the name matches a registered task.
///
/// ## Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
///
/// #[task]
/// async fn send_email(_ctx: TaskContext, email: String) -> Result<String> {
///     Ok(format!("sent to {email}"))
/// }
///
/// #[workflow]
/// async fn my_workflow(ctx: WorkflowContext, email: String) -> Result<String> {
///     // The task is named by its reference, not a string.
///     ctx.execute_task(send_email, email).await
/// }
/// ```
pub trait TaskReference: Send + Sync + 'static {
    /// Return the name the task is registered and scheduled under.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::prelude::*;
    /// use orcher_sdk::task::TaskReference;
    ///
    /// #[task]
    /// async fn send_email(_ctx: TaskContext, email: String) -> Result<String> {
    ///     Ok(email)
    /// }
    ///
    /// assert_eq!(send_email.task_name(), "send_email");
    /// ```
    fn task_name(&self) -> &'static str;

    /// Return a name for logs and debug output.
    ///
    /// Defaults to [`task_name`](Self::task_name). Overriding it does not change
    /// which task is scheduled.
    fn display_name(&self) -> &'static str {
        self.task_name()
    }
}

impl fmt::Debug for dyn TaskReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskReference")
            .field("name", &self.task_name())
            .finish()
    }
}

impl fmt::Display for dyn TaskReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task({})", self.display_name())
    }
}

/// Anything that names a task: a [`TaskReference`] or a `&'static str`.
///
/// APIs that take a task accept either form. A string name is checked only at
/// run time. The trait is sealed and cannot be implemented outside this crate.
///
/// ## Examples
///
/// ```rust
/// use orcher_sdk::prelude::*;
/// use orcher_sdk::task::IntoTaskName;
///
/// #[task]
/// async fn send_email(_ctx: TaskContext, email: String) -> Result<String> {
///     Ok(email)
/// }
///
/// // Accepts a task reference or a string name, like `WorkflowContext::execute_task`.
/// fn task_label<T: IntoTaskName>(task: T) -> String {
///     format!("task:{}", task.into_task_name())
/// }
///
/// assert_eq!(task_label(send_email), "task:send_email"); // Checked at compile time
/// assert_eq!(task_label("send_email"), "task:send_email"); // Checked at run time
/// ```
pub trait IntoTaskName: private::Sealed {
    /// Return the task name.
    fn into_task_name(self) -> &'static str;
}

impl<T: TaskReference> IntoTaskName for T {
    fn into_task_name(self) -> &'static str {
        self.task_name()
    }
}

// String names are accepted for callers without a task reference in scope.
impl IntoTaskName for &'static str {
    fn into_task_name(self) -> &'static str {
        self
    }
}

// Keeps `IntoTaskName` sealed: `private::Sealed` cannot be named outside this crate.
mod private {
    use super::*;

    pub trait Sealed {}

    impl<T: TaskReference> Sealed for T {}
    impl Sealed for &'static str {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone)]
    struct MockTask;

    impl TaskReference for MockTask {
        fn task_name(&self) -> &'static str {
            "mock_task"
        }

        fn display_name(&self) -> &'static str {
            "Mock Task (for testing)"
        }
    }

    #[test]
    fn test_task_reference_name() {
        let task = MockTask;
        assert_eq!(task.task_name(), "mock_task");
    }

    #[test]
    fn test_task_reference_display_name() {
        let task = MockTask;
        assert_eq!(task.display_name(), "Mock Task (for testing)");
    }

    #[test]
    fn test_into_task_name_owned() {
        let task = MockTask;
        assert_eq!(task.into_task_name(), "mock_task");
    }

    #[test]
    fn test_into_task_name_borrowed() {
        let task = MockTask;
        assert_eq!(task.into_task_name(), "mock_task");
    }

    #[test]
    fn test_into_task_name_string() {
        let name: &'static str = "string_task";
        assert_eq!(name.into_task_name(), "string_task");
    }

    #[test]
    fn test_task_reference_debug() {
        let task = MockTask;
        let task_ref: &dyn TaskReference = &task;
        let debug_str = format!("{:?}", task_ref);
        assert!(debug_str.contains("mock_task"));
    }

    #[test]
    fn test_task_reference_display() {
        let task = MockTask;
        let task_ref: &dyn TaskReference = &task;
        let display_str = format!("{}", task_ref);
        assert_eq!(display_str, "Task(Mock Task (for testing))");
    }
}
