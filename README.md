# EventStoreDB Client SDK for Golang [![Actions Status](https://github.com/eventstore/EventStore-Client-Go/workflows/CI/badge.svg?branch=master)](https://github.com/eventstore/EventStore-Client-Go/actions)

**The Go client is in preview, its API is likely to change.**

This repository contains an [EventStoreDB][es] Client SDK written in Go.

## Developing

Integration tests run against a server using Docker, with the [EventStoreDB gRPC Client Test Container][container]. Packages are not currently published to Maven Central, but will be once this library approaches release.

### Setup dependencies

Some dependencies are required in order to work with the code:
* Generated gRPC client, also checked-in but might require an update.
* Certificates for testing TLS requirements, located at `./certs`.

Testing requires [Docker] to be installed. To access the docker images in [GitHub Packages][ghp], you need to authenticate docker with a gitub personal access token. It should be [generated](https://github.com/settings/tokens/new) with at least the following scopes:
- `repo`
- `read:packages`

Then login to the github docker registry with:
```shell
docker login https://docker.pkg.github.com -u YOUR_GITHUB_USERNAME
```

and provide your personal access token as a password. The full instructions can be found in the ["Authenticating to GitHub packages"](https://docs.github.com/en/free-pro-team@latest/packages/guides/configuring-docker-for-use-with-github-packages#authenticating-to-github-packages) guide.

Pull the required docker image:
```shell
docker pull docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata:20.6.0-buster-slim
```

Run docker compose for generating the gRPC client and certificates:
```shell
docker-compose up
docker-compose down
```

### Build from source
```shell
go build ./client
```

### Run tests
```shell
go test ./client
```

## Contributing

All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license.

[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData
[docker]: https://www.docker.com/
[es]: https://eventstore.com
[ghp]: https://github.com/features/packages