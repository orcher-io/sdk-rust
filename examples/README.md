# Examples

Runnable programs that use the SDK in this repository. They are workspace
members, so CI builds, lints and tests them against the current SDK on every
change.

| Example | What it shows |
| --- | --- |
| [`quickstart`](quickstart/src/main.rs) | Two tasks and a workflow, with a worker and a client in one file |
| [`durable_execution`](durable_execution/src/main.rs) | A payment workflow whose side effects run exactly once; separate worker and client modes |
| [`actors`](actors/src/main.rs) | Keyed actors with durable state, called through typed clients |

## Running them

Each example needs a running Orcher engine (gRPC on `localhost:50051` by
default; set `ORCHER_SERVER_URL` to use another) and the namespace it runs in,
which is `examples` for all three. Create it once with the
[CLI](https://github.com/orcher-io/cli): `orcher namespace create examples`.

From the repository root:

```bash
# Start a worker, run one workflow, print its result and exit
cargo run -p orcher-example-quickstart

# A worker and a client as separate processes
cargo run -p orcher-example-durable-execution
cargo run -p orcher-example-durable-execution -- --client

# Actors: worker and client in one process
cargo run -p orcher-example-actors -- --e2e
```

`durable_execution` builds a binary named `durable-execution-service`, which
the engine repository's `scripts/benchmark.sh` and
`scripts/cluster-multinode-e2e.sh` run as their Rust worker. Its flags and
`ORCHER_*` environment variables are listed by `--help`.
