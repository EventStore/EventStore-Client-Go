# KurrentDB Client SDK for Golang [![Actions Status](https://github.com/eventstore/EventStore-Client-Go/workflows/CI/badge.svg?branch=master)](https://github.com/eventstore/EventStore-Client-Go/actions)

KurrentDB is the event-native database, where business events are immutably stored and streamed. Designed for event-sourced, event-driven, and microservices architectures.

This repository contains an [KurrentDB][kurrent] Client SDK written in Go.

## Developing

Integration tests run against a server using Docker, with the [KurrentDB gRPC Client Test Container][container].

### Setup dependencies
Testing requires [Docker] and [Docker Compose] to be installed.

## Build the project

You need [make] to be installed (available on all OSes). On Windows, you need `Powershell`. The version that comes standard with Windows is enough. On a Unix system, any bash
compatible shell should work.

```bash
make build
```

To also regenerate protobuf and gRPC files while building

```bash
make generate-protos-and-build
```

## Run tests

```
make test
```

By default the tests use `docker.eventstore.com/eventstore-ce:ci`. To override this, set the `EVENTSTORE_DOCKER_TAG` environment variable to the tag you wish to use:

```shell
export EVENTSTORE_DOCKER_TAG="21.10.0-focal"
make test
```

## Communities

- [Discuss](https://discuss.eventstore.com/)
- [Discord (Kurrent)](https://discord.gg/Phn9pmCw3t)

## Security

If you find a vulnerability in our software, please contact us. You can find how to reach out us and report it at https://www.eventstore.com/security#security
Thank you very much for supporting our software.

## Contributing

All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license.

[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData

[docker]: https://www.docker.com/
[docker compose]: https://www.docker.com/

[kurrent]: https://kurrent.io
[make]: https://www.gnu.org/software/make/
