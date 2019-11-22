# Event Store Client SDK for Golang [![Actions Status](https://github.com/eventstore/EventStore-Client-Go/workflows/CI/badge.svg?branch=master)](https://github.com/eventstore/EventStore-Client-Go/actions)

This repository contains an [Event Store][es] Client SDK written in Go.

## Developing

Integration tests run against a server using Docker, with the [Event Store gRPC Client Test Container][container]. Packages are not currently published to Maven Central, but will be once this library approaches release.

## Contributing

All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license.

[es]: https://eventstore.com
[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData
