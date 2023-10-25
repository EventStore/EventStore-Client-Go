# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed
- I've refactored parts of the code to allow tests to run without dependencies on functions that create the DB client. [EventStore-Client-Go#152](https://github.com/EventStore/EventStore-Client-Go/pull/152)

### Fixed
- Fix unknown message when reading a subscription. [EventStore-Client-Go#153](https://github.com/EventStore/EventStore-Client-Go/pull/153)

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
