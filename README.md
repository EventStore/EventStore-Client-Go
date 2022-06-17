# EventStoreDB Client SDK for Golang [![Actions Status](https://github.com/eventstore/EventStore-Client-Go/workflows/CI/badge.svg?branch=master)](https://github.com/eventstore/EventStore-Client-Go/actions)

This repository contains an [EventStoreDB][es] Client SDK written in Go.

## Developing

Integration tests run against a server using Docker, with the [EventStoreDB gRPC Client Test Container][container].

### Setup dependencies

Some dependencies are required in order to work with the code:

* Certificates for testing TLS requirements, located at `./certs`.

Testing requires [Docker] and [Docker Compose] to be installed.

## Build the project

On Windows, you need `Powershell`. The version that comes standard with Windows is enough. On a Unix system, any bash
compatible shell should work.

### Windows

```powershell
.\build.ps1
```

### Unix (Linux or OSX)

```bash
./build.sh
```

To also regenerate protobuf and gRPC files while building

### Windows

```powershell
.\build.ps1 -generateProtos
```

### Unix (Linux or OSX)

```bash
./build.sh --generate-protos
```

## Run tests

Run docker compose for generating certificates:

```shell
docker-compose up
docker-compose down
```

```shell
docker-compose -f cluster-docker-compose.yml up -d 
go test ./esdb
docker-compose -f cluster-docker-compose.yml down
```

By default the tests use `ghcr.io/eventstore/eventstore:ci`. To override this, set the `EVENTSTORE_DOCKER_TAG_ENV` environment variable to the tag you wish to use:

```shell
export EVENTSTORE_DOCKER_TAG_ENV="21.10.0-focal"
docker-compose -f cluster-docker-compose.yml up -d 
go test ./esdb
docker-compose -f cluster-docker-compose.yml down
```


## Contributing

All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license.

[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData

[docker]: https://www.docker.com/
[docker compose]: https://www.docker.com/

[es]: https://eventstore.com
