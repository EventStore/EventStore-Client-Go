# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Add unique test in `Error()` for each `ErrorCode`. [EventStore-Client-Go#130](https://github.com/EventStore/EventStore-Client-Go/pull/130)

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