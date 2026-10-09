# Contract suite

A live check that a worker built with this SDK produces correct durable
behavior against a real Orcher engine. It catches the class of bug that unit
tests, type checks and builds cannot: a worker that silently does not run a
workflow, reports a task failure wrongly, or ignores an option.

The assertions are purely client-observable (a workflow's terminal status and
the JSON it returns), and the workflow catalog has the same names in every
Orcher SDK, so the same `scenarios.json` drives the Rust, Python and TypeScript
workers unchanged.

## Layout

- `scenarios.json`: the language-independent contract. Each scenario names a
  catalog workflow, an `input`, and the `expect`ed terminal `status` and
  returned `result`. The other SDKs keep a synced copy of this file.
- `src/catalog.rs`: the workflows and tasks this worker serves. Each workflow
  returns a JSON result describing the durable behavior it observed.
- `src/main.rs`: the driver. It starts the worker, runs every scenario against
  the engine, checks the outcome, and exits non-zero on any failure.
- `src/bin/`: standalone checks that do not fit the scenario format
  (`long_wait`, `stress`, `tenancy`).

## Running

The suite runs in a namespace called `contract`, not `default`, and creates it
on start if the engine does not have it yet.

Using a namespace other than `default` makes namespace mistakes visible: a
child workflow created in `default` instead of its parent's namespace is
stranded where no worker claims it, but in `default` itself the right namespace
and the wrong one are the same string, so the child-workflow scenarios would
pass either way. Set `ORCHER_NAMESPACE` to use a different namespace.

Start an Orcher engine with its gRPC API on port 50051 and durable task
failures enabled (`ORCHER_DURABLE_TASK_FAILURE=true`).

The worker-liveness scenarios (`task-outlives-heartbeat-timeout-without-heartbeat-code`,
`task-retried-after-its-worker-dies`) need the engine to time out a silent task
within seconds rather than its default of a minute. Start the engine with
`ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS=3 ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS=1
ORCHER_TIMEOUT_WATCHER_GRACE_SECS=1`; with the defaults the first fails and the
second takes a couple of minutes.

Then, from the repository root:

```bash
./contract/run.sh
# or directly:
cargo run -p orcher-contract -- --server-url http://localhost:50051
```

## Adding a scenario

1. Add the workflow (and any tasks) to `src/catalog.rs`, returning a JSON
   result that encodes the behavior under test. Use the same names and result
   shapes in the other SDKs' catalogs.
2. Add an entry to `scenarios.json` with the expected terminal status and
   result.

Result matching is structural: a scenario asserts only the fields it lists, and
extra fields in the actual result are ignored.
