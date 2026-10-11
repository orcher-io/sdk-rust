# Changelog

## [0.9.1](https://github.com/orcher-io/sdk-rust/compare/v0.9.0...v0.9.1) (2026-10-11)


### Bug Fixes

* derive a child workflow's default id from its parent ([#40](https://github.com/orcher-io/sdk-rust/issues/40)) ([3dbd503](https://github.com/orcher-io/sdk-rust/commit/3dbd503caa45c9346b8ef43fade2ab967f9ff1c5)), closes [#25](https://github.com/orcher-io/sdk-rust/issues/25)
* raise TaskFailed from execute_task_with_options ([#41](https://github.com/orcher-io/sdk-rust/issues/41)) ([3aab407](https://github.com/orcher-io/sdk-rust/commit/3aab4072e9ce73edf0576da123a7bd083971fbe9)), closes [#38](https://github.com/orcher-io/sdk-rust/issues/38)

## [0.9.0](https://github.com/orcher-io/sdk-rust/compare/v0.8.1...v0.9.0) (2026-10-11)


### ⚠ BREAKING CHANGES

* namespace on a task or task group is now a compile error, as it already is on a workflow, and an unknown preset name is a compile error instead of being ignored.

### Bug Fixes

* run a workflow or task that takes no input ([#35](https://github.com/orcher-io/sdk-rust/issues/35)) ([85e79c8](https://github.com/orcher-io/sdk-rust/commit/85e79c88d874276d1c271a39875342d58180f0ab))
* schedule a task under its declared name ([#34](https://github.com/orcher-io/sdk-rust/issues/34)) ([5c0a0c8](https://github.com/orcher-io/sdk-rust/commit/5c0a0c814cbd91d804f48a60178c906b4cdffac1))
* warn about task and workflow attributes that have no effect ([#36](https://github.com/orcher-io/sdk-rust/issues/36)) ([cdd942a](https://github.com/orcher-io/sdk-rust/commit/cdd942adc9e2c689ce55611938275f988f5339e5))

## [0.8.1](https://github.com/orcher-io/sdk-rust/compare/v0.8.0...v0.8.1) (2026-10-09)


### Features

* let a workflow clean up when it is cancelled ([#31](https://github.com/orcher-io/sdk-rust/issues/31)) ([684c4b3](https://github.com/orcher-io/sdk-rust/commit/684c4b357c3e628de14a5a2bacc87db64faeffac))

## [0.8.0](https://github.com/orcher-io/sdk-rust/compare/v0.7.1...v0.8.0) (2026-10-09)


### ⚠ BREAKING CHANGES

* the sdk-core types this crate exposes, such as `WorkflowStatus`, the re-exported listing types and `NamespaceInfo`, now come from orcher-sdk-core 0.10 and orcher-proto 0.2.

### Features

* build on orcher-sdk-core 0.10, with a cleanup limit on cancellation ([#28](https://github.com/orcher-io/sdk-rust/issues/28)) ([91f79de](https://github.com/orcher-io/sdk-rust/commit/91f79de8d810b32b05c781e2f0bbf2f0a8573f7c))

## [0.7.1](https://github.com/orcher-io/sdk-rust/compare/v0.7.0...v0.7.1) (2026-10-08)


### Features

* TLS and mTLS for workers, automatic TLS for https:// URLs ([#26](https://github.com/orcher-io/sdk-rust/issues/26)) ([9f4ec5d](https://github.com/orcher-io/sdk-rust/commit/9f4ec5d7efb557f5abd5b95058991dc5531813fb)), closes [#17](https://github.com/orcher-io/sdk-rust/issues/17) [#18](https://github.com/orcher-io/sdk-rust/issues/18)

## [0.7.0](https://github.com/orcher-io/sdk-rust/compare/v0.6.0...v0.7.0) (2026-10-07)


### ⚠ BREAKING CHANGES

* reject namespace on #[workflow], warn on timeout and version, deregister on shutdown ([#13](https://github.com/orcher-io/sdk-rust/issues/13))

### Bug Fixes

* reject namespace on #[workflow], warn on timeout and version, deregister on shutdown ([#13](https://github.com/orcher-io/sdk-rust/issues/13)) ([f4f1012](https://github.com/orcher-io/sdk-rust/commit/f4f1012ec65420c052d3ba5f5cd19262a06085e3))

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
