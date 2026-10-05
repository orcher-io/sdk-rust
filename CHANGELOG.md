# Changelog

## [0.6.0](https://github.com/orcher-io/sdk-rust/compare/v0.5.2...v0.6.0) (2026-10-05)


### ⚠ BREAKING CHANGES

* the sdk-core types this crate exposes, such as the `ExecutionResult` that `ExecutionRuntime::execute_workflow` returns and the re-exported listing types, now come from orcher-sdk-core 0.9.

### Bug Fixes

* report the steps each activation reached, and build on orcher-sdk-core 0.9.0 so code that no longer replays a run is caught ([#12](https://github.com/orcher-io/sdk-rust/issues/12)) ([a5f93af](https://github.com/orcher-io/sdk-rust/commit/a5f93af298fc7101da4378440fdb57a40695e0f4))


### Documentation

* read the version badge from the registry with a short cache ([#10](https://github.com/orcher-io/sdk-rust/issues/10)) ([076c3e1](https://github.com/orcher-io/sdk-rust/commit/076c3e1d2ff7261415b466f807d6078b2eb47da5))

## [0.5.2](https://github.com/orcher-io/sdk-rust/compare/v0.5.1...v0.5.2) (2026-10-04)


### Bug Fixes

* show the README banner on the package registries ([#8](https://github.com/orcher-io/sdk-rust/issues/8)) ([8097590](https://github.com/orcher-io/sdk-rust/commit/80975902d431e4643885a6b7991ad0417e475987))

## [0.5.1](https://github.com/orcher-io/sdk-rust/compare/v0.5.0...v0.5.1) (2026-10-04)


### Bug Fixes

* build on orcher-sdk-core 0.8.1 for 32 MiB messages, and count task attempts from 1 ([#7](https://github.com/orcher-io/sdk-rust/issues/7)) ([92d5e7c](https://github.com/orcher-io/sdk-rust/commit/92d5e7c249bbf3c2adb99d461f6419dfa4248242))


### Documentation

* **contract:** describe the restart-fresh scenario in ORCHER's own terms ([#5](https://github.com/orcher-io/sdk-rust/issues/5)) ([3263e3d](https://github.com/orcher-io/sdk-rust/commit/3263e3dbed25815e2d4f561628b8cb64d5edc369))
* link private vulnerability reporting from CONTRIBUTING ([#6](https://github.com/orcher-io/sdk-rust/issues/6)) ([f1e85c1](https://github.com/orcher-io/sdk-rust/commit/f1e85c17d1cf820b5d16b2c661a9dc853e84b7ca))
* show the Rust logo beside the banner's SDK label ([#3](https://github.com/orcher-io/sdk-rust/issues/3)) ([b93fce9](https://github.com/orcher-io/sdk-rust/commit/b93fce9caaa56e72f4612df32520a0ca3f6befb2))

## Changelog
