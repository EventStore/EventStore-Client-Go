# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

### Updated
- Update dependencies. [EventStore-Client-Go#172](https://github.com/EventStore/EventStore-Client-Go/pull/172)

### Changed
- Updated CI workflows to pull eventstore docker images from cloud smith registry. [EventStore-Client-Go#165](https://github.com/EventStore/EventStore-Client-Go/pull/165)
- Updated README to reference Cloudsmith repository instead of ghcr. [EventStore-Client-Go#165](https://github.com/EventStore/EventStore-Client-Go/pull/165)

## [4.0.0] - 2024-03-03
### Added
- Add missing persistent subscription API samples. [EventStore-Client-Go#168](https://github.com/EventStore/EventStore-Client-Go/pull/168)
- Implement projections API. [EventStore-Client-Go#167](https://github.com/EventStore/EventStore-Client-Go/pull/167)

### Changed
- Move to google uuid implementation. [EventStore-Client-Go#169](https://github.com/EventStore/EventStore-Client-Go/pull/169)
- Move to version 4. [EventStore-Client-Go#170](https://github.com/EventStore/EventStore-Client-Go/pull/170)

## [3.3.0] - 2024-01-27
### Added
- CaughtUp and FellBehind message handling in subscription [EventStore-Client-Go#161](https://github.com/EventStore/EventStore-Client-Go/pull/161)
- OriginalStreamRevision to ResolvedEvent [EventStore-Client-Go#163](https://github.com/EventStore/EventStore-Client-Go/pull/163)
- `IsErrorCode` to `Error` struct. [EventStore-Client-Go#162](https://github.com/EventStore/EventStore-Client-Go/pull/162)

## [3.2.1] - 2023-11-28
### Fixed
- Fix race condition and overwriting when setting call credentials [EventStore-Client-Go#160](https://github.com/EventStore/EventStore-Client-Go/pull/160)

## [3.2.0] - 2023-10-31
### Changed
- Improve CI separation in Github Actions. [EventStore-Client-Go#152](https://github.com/EventStore/EventStore-Client-Go/pull/152)
- Improve go client connection string parsing [EventStore-Client-Go#156](https://github.com/EventStore/EventStore-Client-Go/pull/156)
- Update dependencies. [EventStore-Client-Go#157](https://github.com/EventStore/EventStore-Client-Go/pull/157)

### Fixed
- Fix unknown message when reading a subscription. [EventStore-Client-Go#153](https://github.com/EventStore/EventStore-Client-Go/pull/153)
- Retry container creation on health check failure during tests [EventStore-Client-Go#154](https://github.com/EventStore/EventStore-Client-Go/pull/154)
- Fix nil deref when reading last checkpointed event revision from a persistent subscription. [EventStore-Client-Go#151](https://github.com/EventStore/EventStore-Client-Go/pull/151)

## [3.1.0] - 2023-07-18

### Added
- Add unique test in `Error()` for each `ErrorCode`. [EventStore-Client-Go#130](https://github.com/EventStore/EventStore-Client-Go/pull/130)

### Changed
- Increase max receive message length to match the max event size in EventStoreDB [EventStore-Client-Go#135](https://github.com/EventStore/EventStore-Client-Go/pull/135)
- Update container version when testing. [EventStore-Client-Go#137](https://github.com/EventStore/EventStore-Client-Go/pull/137)

### Fixed
- Do not start discovery process on ABORT gRPC error. [EventStore-Client-Go#134](https://github.com/EventStore/EventStore-Client-Go/pull/134)
- Configuring `WithPerRPCCredentials` more than once causes authentication to fail. [EventStore-Client-Go#136](https://github.com/EventStore/EventStore-Client-Go/pull/136)
- Stop using deprecated runner. [EventStore-Client-Go#139](https://github.com/EventStore/EventStore-Client-Go/pull/139)
- CI build stuck because specified runner is not available anymore. [EventStore-Client-Go#140](https://github.com/EventStore/EventStore-Client-Go/pull/140)

## [3.0.0] - 2022-07-20

### Added
- Document public API and types. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)

### Changed
- Rename `ContentType` enum. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)
- Rename `ErrorCode` enum. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)
- Rename `NodePreference` enum. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)
- Rename `NackAction` enum. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)
- Rename `SubscriptionSettings` struct. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)
- Rename `ConsumerStrategy` enum. [EventStore-Client-Go#120](https://github.com/EventStore/EventStore-Client-Go/pull/120)

### Fixed
- Fix RequiresLeader header when node preference is set to Leader. [EventStore-Client-Go#126](https://github.com/EventStore/EventStore-Client-Go/pull/126)
- Respect `MaxDiscoverAttempts` setting. [EventStore-Client-Go#122](https://github.com/EventStore/EventStore-Client-Go/pull/122)
