#!/usr/bin/env bash
# Run the Rust contract worker + driver against a running engine.
#
# Expects an Orcher engine to be listening already on localhost, at the port
# of ORCHER_SERVER_URL (default http://localhost:50051); it does not start one.
set -euo pipefail

SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}"
PORT="${SERVER_URL##*:}"

cd "$(dirname "$0")/.."

if ! nc -z localhost "$PORT" 2>/dev/null; then
  echo "error: no engine reachable on localhost:$PORT" >&2
  echo "       start one from the orcher repo: cargo run --release --bin orchestrator" >&2
  exit 2
fi

# Name the binary explicitly: the package also ships `long_wait` and `stress`,
# and without this `cargo run -p` is ambiguous and refuses to start.
exec cargo run -q -p orcher-contract --bin orcher-contract -- --server-url "$SERVER_URL" "$@"
