# Changelog

## [0.13.1](https://github.com/orcher-io/sdk-rust/compare/v0.13.0...v0.13.1) (2026-10-24)


### Bug Fixes

* log a workflow's task-result lookup at trace, not warn ([#85](https://github.com/orcher-io/sdk-rust/issues/85)) ([b5a9da7](https://github.com/orcher-io/sdk-rust/commit/b5a9da704ca2e2f3aabe82036cc5eeb85a00003c))

## [0.13.0](https://github.com/orcher-io/sdk-rust/compare/v0.12.0...v0.13.0) (2026-10-23)


### ⚠ BREAKING CHANGES

* run on sdk-core 0.20 ([#83](https://github.com/orcher-io/sdk-rust/issues/83))

### Features

* run on sdk-core 0.20 ([#83](https://github.com/orcher-io/sdk-rust/issues/83)) ([eec72f8](https://github.com/orcher-io/sdk-rust/commit/eec72f895b0dc780c5feab42974de7b0a4bfcb0e))

## [0.12.0](https://github.com/orcher-io/sdk-rust/compare/v0.11.4...v0.12.0) (2026-10-21)


### Features

* expose the worker's measurements of the engine wire and its work ([#79](https://github.com/orcher-io/sdk-rust/issues/79)) ([1d94525](https://github.com/orcher-io/sdk-rust/commit/1d945250fb4310ac1b976708e703e83cdcabae59))
* move to sdk-core 0.19 ([#81](https://github.com/orcher-io/sdk-rust/issues/81)) ([bf06e51](https://github.com/orcher-io/sdk-rust/commit/bf06e51b7c7fa61ee0d347f7965abf843475a5e3))


### Documentation

* say a move to a newer orcher-sdk-core is a minor release ([#82](https://github.com/orcher-io/sdk-rust/issues/82)) ([a3d1acb](https://github.com/orcher-io/sdk-rust/commit/a3d1acba0429deea7a08f4d94e19416392b19374))

## [0.11.4](https://github.com/orcher-io/sdk-rust/compare/v0.11.3...v0.11.4) (2026-10-19)


### Bug Fixes

* mark a workflow failure non-retryable only when its code raised it so ([#76](https://github.com/orcher-io/sdk-rust/issues/76)) ([7789f26](https://github.com/orcher-io/sdk-rust/commit/7789f26177242790d8f42d173ae4d0c5a7e07be2))
* send the calls a failing workflow made before it failed ([#78](https://github.com/orcher-io/sdk-rust/issues/78)) ([d2bb054](https://github.com/orcher-io/sdk-rust/commit/d2bb05480852eed53305aa8b38be6d071218af59))

## [0.11.3](https://github.com/orcher-io/sdk-rust/compare/v0.11.2...v0.11.3) (2026-10-18)


### Features

* let a child workflow and a restart-fresh set their retry policy ([#73](https://github.com/orcher-io/sdk-rust/issues/73)) ([5bd22d7](https://github.com/orcher-io/sdk-rust/commit/5bd22d7dfaf062e31d4ecffd1708d30841e77cb7)), closes [#72](https://github.com/orcher-io/sdk-rust/issues/72)

## [0.11.2](https://github.com/orcher-io/sdk-rust/compare/v0.11.1...v0.11.2) (2026-10-17)


### Features

* expose a task failure's error type and whether it was cancelled ([#70](https://github.com/orcher-io/sdk-rust/issues/70)) ([5979ab6](https://github.com/orcher-io/sdk-rust/commit/5979ab6e95c36047a77d4396b3b0db7ab02bf4fa))


### Bug Fixes

* deliver a cancellation by the kernel's order of outcomes ([#67](https://github.com/orcher-io/sdk-rust/issues/67)) ([9623a62](https://github.com/orcher-io/sdk-rust/commit/9623a62617d49b481b9163a7bcb7f2178253b9b2))

## [0.11.1](https://github.com/orcher-io/sdk-rust/compare/v0.11.0...v0.11.1) (2026-10-15)


### Features

* add runnable examples built against the current SDK ([#64](https://github.com/orcher-io/sdk-rust/issues/64)) ([fa10602](https://github.com/orcher-io/sdk-rust/commit/fa10602adcb8a4f10a2d3507922c8bf9d67aaa86))


### Bug Fixes

* keep a restart's execution timeout on the worker host ([#62](https://github.com/orcher-io/sdk-rust/issues/62)) ([b5bea62](https://github.com/orcher-io/sdk-rust/commit/b5bea62f0bf83775f1cf8657371cc59e86b85b02))
* read a workflow run's attempt as the engine numbers it ([#66](https://github.com/orcher-io/sdk-rust/issues/66)) ([eb5b6e4](https://github.com/orcher-io/sdk-rust/commit/eb5b6e494cacf29bbd33a36a8d2da63dff3f0851))
* send a workflow retry policy's non-retryable errors on start ([#68](https://github.com/orcher-io/sdk-rust/issues/68)) ([7e09b5f](https://github.com/orcher-io/sdk-rust/commit/7e09b5f5797c5cd2effc4cda2f43defdb9f9a3b0))

## [0.11.0](https://github.com/orcher-io/sdk-rust/compare/v0.10.0...v0.11.0) (2026-10-14)


### ⚠ BREAKING CHANGES

* ExecutionRuntime::execute_workflow is removed; run an activation with ExecutionRuntime::run_activation.

### Features

* remove the journal-reading workflow path ([#58](https://github.com/orcher-io/sdk-rust/issues/58)) ([90c8c7e](https://github.com/orcher-io/sdk-rust/commit/90c8c7e672033bcacbf9dbf167bf07bb082f1100))

## [0.10.0](https://github.com/orcher-io/sdk-rust/compare/v0.9.1...v0.10.0) (2026-10-14)


### ⚠ BREAKING CHANGES

* worker::session (the worker-side SessionManager) is removed; sessions run in the host. Actor handlers are registered when the worker runs, so a refusal is Worker::run's StartupFailed rather than build()'s. A worker that registered nothing to run is refused at run().
* QueryRejectCondition and
* a test that matched the StateError a failing mock used to stop the run with now receives WorkflowError::TaskFailed.
* remove StartWorkflowOptions::with_idempotency_key ([#45](https://github.com/orcher-io/sdk-rust/issues/45))
* report a compensation input that cannot be serialized ([#44](https://github.com/orcher-io/sdk-rust/issues/44))

### Features

* let a task body be unit-tested with TestTaskContext ([#49](https://github.com/orcher-io/sdk-rust/issues/49)) ([0ed24d2](https://github.com/orcher-io/sdk-rust/commit/0ed24d227f397d54f87e9c25b4fd0bfb2af77397)), closes [#24](https://github.com/orcher-io/sdk-rust/issues/24)
* run the worker on the shared worker host ([#57](https://github.com/orcher-io/sdk-rust/issues/57)) ([b798e9e](https://github.com/orcher-io/sdk-rust/commit/b798e9eb3971636c840019de815761c19356ab31))
* run workflow activations from the kernel protocol ([#55](https://github.com/orcher-io/sdk-rust/issues/55)) ([abf93c9](https://github.com/orcher-io/sdk-rust/commit/abf93c9072ffd9f164421d8bd61d34e1a13095e0))


### Bug Fixes

* deliver a mocked task failure to the workflow as TaskFailed ([#51](https://github.com/orcher-io/sdk-rust/issues/51)) ([4a31af8](https://github.com/orcher-io/sdk-rust/commit/4a31af825cfca842a2998adc7766173713d998a0)), closes [#24](https://github.com/orcher-io/sdk-rust/issues/24)
* pass query arguments to handlers and apply query and event timeouts ([#52](https://github.com/orcher-io/sdk-rust/issues/52)) ([6fe67b9](https://github.com/orcher-io/sdk-rust/commit/6fe67b932185f53f7b81d1ceb09a2b373b8bc80b)), closes [#24](https://github.com/orcher-io/sdk-rust/issues/24)
* remove StartWorkflowOptions::with_idempotency_key ([#45](https://github.com/orcher-io/sdk-rust/issues/45)) ([bea5fd3](https://github.com/orcher-io/sdk-rust/commit/bea5fd3181165711a4ef3fa403a8a277405cf62b)), closes [#19](https://github.com/orcher-io/sdk-rust/issues/19)
* report a compensation input that cannot be serialized ([#44](https://github.com/orcher-io/sdk-rust/issues/44)) ([43cafdd](https://github.com/orcher-io/sdk-rust/commit/43cafdd724e0f70c98ca8295332671ee92e713b6)), closes [#39](https://github.com/orcher-io/sdk-rust/issues/39)
* stop logging every task replay at warn level ([#48](https://github.com/orcher-io/sdk-rust/issues/48)) ([b78f856](https://github.com/orcher-io/sdk-rust/commit/b78f8560e517240581f89c9d689729c145efa348))
* wait for a session slot and report a failed creation as TaskFailed ([#47](https://github.com/orcher-io/sdk-rust/issues/47)) ([725e369](https://github.com/orcher-io/sdk-rust/commit/725e3690020d6aa6a567b4ed33130458a7713f97)), closes [#22](https://github.com/orcher-io/sdk-rust/issues/22)


### Documentation

* say heartbeat details do not reach a retried attempt ([#53](https://github.com/orcher-io/sdk-rust/issues/53)) ([02557de](https://github.com/orcher-io/sdk-rust/commit/02557de239af35295d52f7844615f3188b94b947)), closes [#24](https://github.com/orcher-io/sdk-rust/issues/24)
* say shared actor operations run concurrently ([#46](https://github.com/orcher-io/sdk-rust/issues/46)) ([209a5fc](https://github.com/orcher-io/sdk-rust/commit/209a5fc876c859484c0e5e2a000e66ed71a54894)), closes [#23](https://github.com/orcher-io/sdk-rust/issues/23)

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
