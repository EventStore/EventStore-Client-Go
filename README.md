# EventStoreDB Client SDK written in Golang [![Actions Status](https://github.com/pivonroll/EventStore-Client-Go/workflows/CI/badge.svg?branch=develop)](https://github.com/pivonroll/EventStore-Client-Go/actions)

**This is a fork of the EventStore's Golang Client for EventStoreDB**

## Supported EventstoreDB Version 
**Supports Eventstore 21.6.0**

## Supported Features
* Event Streams
  * Append
  * Tombstone (hard delete)
  * Delete (soft delete)
  * Get/Set Stream Metadata
  * Read Stream Events
  * Read All Events
  * Subscribe To Stream
  * Subscribe To All
* Persistent Subscriptions
  * Create/Update/Delete
  * Subscribe To Persistent Subscription
* Projections
  * Create/Update/Delete Projections
  * Enable/Abort/Disable/Reset Projection
  * Get Projection's State/Result/Statistics
  * Restart Projection Subsystem
  * List All/Continuous/One Time Projections
* User Management
  * Create/Update/Delete User
  * Enable/Disable User
  * Get User's Details
  * List All Users
  * Change User's Password
  * Reset Users Password

## Roadmap
* Add support for async read of events from event streams
* Add support for async read of events from persistent subscriptions
* Add support for Operations

## Developing

Integration tests run against a server using Docker, with the [EventStoreDB gRPC Client Test Container][container]. 
New releases are published on github.com/pivonroll/EventStore-Client-Go
To run integration tests you need to have [Docker] installed and running.

Starting docker in most Linux distros is done through systemctl.
For example:
```shell
$ sudo systemctl start docker.service
```
If your Linux distro does not use systemctl, check what it uses and how to start docker on it.  

### Setup dependencies
Some dependencies are required in order to work with the code

#### 1. Generate GRPC Client from proto

In order to generate Golang GRPC Client code from proto, run:

Windows
```powershell
.\build.ps1 -generateProtos
```

Unix (Linux or OSX)
```bash
./build.sh --generate-protos
```
<br>

#### 2. Generate certificates for testing TLS requirements
To generate certificates for TLS you need to run (from projects main directory)

```shell
$ sudo docker-compose up
```

The docker container will generate TLS certificates and put them in `./certs` directory.

<br>

#### 3. Fetch EventstoreDB container with available pre-populated database
 To access the docker images in [GitHub Packages][ghp], you need to authenticate docker with a gitub personal access token. It should be [generated](https://github.com/settings/tokens/new) with at least the following scopes:
- `repo`
- `read:packages`

Then login to the github docker registry with:
```shell
$ sudo docker login https://docker.pkg.github.com -u YOUR_GITHUB_USERNAME
```
or 
```shell
$ sudo docker login ghcr.io  -u YOUR_GITHUB_USERNAME
```
and provide your personal access token as a password. 

Alternatively you can store your access token in a file (example: `~/.accesstoken`) and login by executing:
```shell
$ cat ~/.accesstoken | docker login ghcr.io -u USERNAME --password-stdin
```

The full instructions can be found in the ["Authenticating to GitHub packages"](https://docs.github.com/en/free-pro-team@latest/packages/guides/configuring-docker-for-use-with-github-packages#authenticating-to-github-packages) guide.

Pull the required docker image (should be 21.6.0):
```shell
$ sudo docker pull docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata:21.6.0-buster-slim
```
## Build the project
On Windows, you need `Powershell`. The version that comes standard with Windows is enough.
On a Unix system, any bash compatible shell should work.

### Windows
```powershell
.\build.ps1
```

### Unix (Linux or OSX)
```bash
./build.sh
```

## Run tests
```shell
go test ./client
```

## Contributing
All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license.

[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData
[docker]: https://www.docker.com/
[es]: https://eventstore.com
[ghp]: https://github.com/features/packages