.DEFAULT_GOAL := help

help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

OS := $(shell uname)

GENERATE_PROTOS_FLAG=
CI_TARGET=

.PHONY: build
build: ## Build based on the OS.
ifeq ($(OS),Linux)
	./build.sh $(GENERATE_PROTOS_FLAG)
else ifeq ($(OS),Darwin)
	./build.sh $(GENERATE_PROTOS_FLAG)
else
	pwsh.exe -File ".\build.ps1" $(GENERATE_PROTOS_FLAG)
endif

.PHONY: generate-protos-and-build
generate-protos-and-build: ## Generate protobuf and gRPC files while building.
ifeq ($(OS),Linux)
	$(MAKE) build GENERATE_PROTOS_FLAG=--generate-protos
else ifeq ($(OS),Darwin)
	$(MAKE) build GENERATE_PROTOS_FLAG=--generate-protos
else
	$(MAKE) build GENERATE_PROTOS_FLAG=-generateProtos
endif

DOCKER_COMPOSE_CMD := $(shell command -v docker compose 2> /dev/null)
ifeq ($(DOCKER_COMPOSE_CMD),)
	DOCKER_COMPOSE_CMD := docker compose
endif

.PHONY: singleNode
singleNode: ## Run tests against a single node.
	@EVENTSTORE_INSECURE=true go test -count=1 -v ./esdb -run 'TestStreams|TestPersistentSubscriptions|TestExpectations|TestProjections'

.PHONY: secureNode
secureNode: ## Run tests against a secure node.
	@$(DOCKER_COMPOSE_CMD) down -v
	@$(DOCKER_COMPOSE_CMD) pull
	@$(DOCKER_COMPOSE_CMD) up -d
	@EVENTSTORE_INSECURE=false go test -v ./esdb -run 'TestStreams|TestPersistentSubscriptions|TestProjections'
	@$(DOCKER_COMPOSE_CMD) down

.PHONY: clusterNode
clusterNode: ## Run tests against a cluster node.
	@$(DOCKER_COMPOSE_CMD) -f cluster-docker-compose.yml down --remove-orphans -v
	@$(DOCKER_COMPOSE_CMD) -f cluster-docker-compose.yml pull
	@$(DOCKER_COMPOSE_CMD) -f cluster-docker-compose.yml up -d
	@echo "Waiting for services to be fully ready..."
	@sleep 5
	@EVENTSTORE_INSECURE=false CLUSTER=true go test -count=1 -v ./esdb -run 'TestStreams|TestPersistentSubscriptions|TestProjections'
	@$(DOCKER_COMPOSE_CMD) -f cluster-docker-compose.yml down --remove-orphans

.PHONY: misc
misc: ## Run tests that don't need a server to run.
	go test -v ./esdb -run TestMisc

.PHONY: test
test: singleNode secureNode clusterNode misc ## Run all tests.

.PHONY: ci
ci: ## Run tests in Github Actions setting.
	go test -v ./esdb -run "$(CI_TARGET)"
