# Contributing

Thanks for helping improve the Orcher Rust SDK. Bug reports, documentation
fixes and code changes are all welcome.

## Reporting a problem

Open an issue with the SDK version, what you expected, what happened, and the
smallest code that shows it. Ask questions in
[Discussions](https://github.com/orcher-io/quickstart/discussions). For a
suspected security problem, do not open a public issue;
[report it privately](https://github.com/orcher-io/sdk-rust/security/advisories/new)
instead.

## Setting up

You need a stable Rust toolchain with `rustfmt` and `clippy`. Nothing else is
required: the protocol crate compiles its definitions itself.

```bash
cargo build --workspace
```

To work on this SDK and its core crates together, copy
`.cargo/config.example.toml` to `.cargo/config.toml`. It builds against sibling
`../sdk-core` and `../protos` checkouts instead of the crates.io releases.

## Checks

Every pull request must pass the same checks CI runs:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-features --lib --tests
cargo test --workspace --all-features --doc
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Documentation examples are tests. Make a new one compile and, where it can,
run; mark it `no_run` only when it needs a running engine.

Changes to how the worker behaves against a real engine should also pass the
contract suite in [`contract/`](contract/README.md), which needs a running
Orcher engine.

## Pre-commit hooks

The repository uses [pre-commit](https://pre-commit.com) for fast local
checks. Install the hooks once per clone:

```bash
pipx install pre-commit      # or: brew install pre-commit / pip install pre-commit
pre-commit install           # installs the pre-commit and commit-msg git hooks
```

They then run on every `git commit`, against the changed files only. Run them
by hand with `pre-commit run --all-files`.

| Hook | What it does |
|------|--------------|
| trailing-whitespace, end-of-file-fixer, mixed-line-ending | whitespace hygiene |
| check-merge-conflict, check-added-large-files, check-yaml, check-toml | guardrails |
| detect-private-key, **gitleaks** | secret scanning |
| **typos** | spell-check (allowlist in `_typos.toml`) |
| **rustfmt** | formats changed Rust files |
| **conventional-pre-commit** | enforces `type(scope): subject` commit messages |

## Commit messages and pull requests

Commits follow [Conventional Commits](https://www.conventionalcommits.org):
`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`. Mark a breaking
change with `!` (`feat!: ...`) or a `BREAKING CHANGE:` footer. Releases and the
changelog are generated from these messages, so write the subject for a reader
of the changelog.

Keep a pull request to one change, explain why it is needed, and add a test
that fails without it where that is possible.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License, Version 2.0](LICENSE).
