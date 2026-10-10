//! Compile-time checks on what the macros accept and report.
//!
//! The expected compiler output lives next to each case in `tests/ui/`. After an
//! intended change to a message, regenerate it with `TRYBUILD=overwrite cargo test
//! -p orcher-sdk-macros --test ui`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/workflow_ineffective_attrs.rs");
    t.compile_fail("tests/ui/task_ineffective_attrs.rs");
    t.compile_fail("tests/ui/task_namespace.rs");
}
