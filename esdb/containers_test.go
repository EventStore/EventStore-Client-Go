package esdb_test

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/davecgh/go-spew/spew"
	"github.com/goombaio/namegenerator"
)

const (
	ESDB_DOCKER_REPO_ENV              = "ESDB_DOCKER_REPO"
	ESDB_DOCKER_CONTAINER_ENV         = "ESDB_DOCKER_CONTAINER"
	ESDB_DOCKER_CONTAINER_VERSION_ENV = "ESDB_DOCKER_CONTAINER_VERSION"
	EVENTSTORE_DOCKER_PORT_ENV        = "EVENTSTORE_DOCKER_PORT"
)

var (
	NAME_GENERATOR = namegenerator.NewNameGenerator(0)
)

// Container ...
type Container struct {
	Endpoint  string
	Container testcontainers.Container
}

type EventStoreDockerConfig struct {
	Repository string
	Tag        string
	Port       string
	Insecure   bool
}

const (
	DEFAULT_ESDB_DOCKER_REPO              = "eventstore-ce"
	DEFAULT_ESDB_DOCKER_CONTAINER         = "eventstoredb-ce"
	DEFAULT_ESDB_DOCKER_CONTAINER_VERSION = "latest"
	DEFAULT_EVENTSTORE_DOCKER_PORT        = "2113"
)

func fullDockerRepo(repo string, container string) string {
	return fmt.Sprintf("docker.eventstore.com/%s/%s", repo, container)
}

var defaultEventStoreDockerConfig = EventStoreDockerConfig{
	Repository: fullDockerRepo(DEFAULT_ESDB_DOCKER_REPO, DEFAULT_ESDB_DOCKER_CONTAINER),
	Tag:        DEFAULT_ESDB_DOCKER_CONTAINER_VERSION,
	Port:       DEFAULT_EVENTSTORE_DOCKER_PORT,
}

func GetEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func readEnvironmentVariables(config EventStoreDockerConfig) EventStoreDockerConfig {
	repo := GetEnvOrDefault(ESDB_DOCKER_REPO_ENV, DEFAULT_ESDB_DOCKER_REPO)
	container := GetEnvOrDefault(ESDB_DOCKER_CONTAINER_ENV, DEFAULT_ESDB_DOCKER_CONTAINER)

	config.Repository = fullDockerRepo(repo, container)
	config.Tag = GetEnvOrDefault(ESDB_DOCKER_CONTAINER_VERSION_ENV, config.Tag)
	config.Port = GetEnvOrDefault(EVENTSTORE_DOCKER_PORT_ENV, config.Port)

	fmt.Println(spew.Sdump(config))
	return config
}

func getContainerRequest() (*EventStoreDockerConfig, *testcontainers.ContainerRequest, error) {
	config := readEnvironmentVariables(defaultEventStoreDockerConfig)

	env := map[string]string{}
	var files []testcontainers.ContainerFile
	insecure, err := strconv.ParseBool(GetEnvOrDefault("EVENTSTORE_INSECURE", "true"))

	if err != nil {
		insecure = true
	}

	if !insecure {

		err := verifyCertificatesExist()

		if err != nil {
			return nil, nil, err
		}

		certsDir, err := getCertificatesDir()

		if err != nil {
			return nil, nil, err
		}

		env["EVENTSTORE_CERTIFICATE_FILE"] = "/etc/eventstore/certs/node/node.crt"
		env["EVENTSTORE_CERTIFICATE_PRIVATE_KEY_FILE"] = "/etc/eventstore/certs/node/node.key"
		env["EVENTSTORE_TRUSTED_ROOT_CERTIFICATES_PATH"] = "/etc/eventstore/certs/ca"

		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      certsDir,
			ContainerFilePath: "/etc/eventstore/certs",
			FileMode:          int64(0755),
		})
	}

	env["EVENTSTORE_INSECURE"] = strconv.FormatBool(insecure)
	env["EVENTSTORE_RUN_PROJECTIONS"] = "all"
	env["EVENTSTORE_START_STANDARD_PROJECTIONS"] = "true"
	env["EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP"] = "true"

	return &config, &testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("%s:%s", config.Repository, config.Tag),
		ExposedPorts: []string{config.Port},
		Env:          env,
		Files:        files,
		WaitingFor: wait.
			ForHTTP("/health/live").
			WithPort(nat.Port(config.Port)).
			WithTLS(!insecure).
			WithStartupTimeout(1 * time.Minute).
			WithAllowInsecure(true).
			WithStatusCodeMatcher(func(status int) bool {
				return status >= 200 && status < 300
			}),
	}, nil
}

func (container *Container) Close() {
	timeout := 1 * time.Minute
	err := container.Container.Stop(context.Background(), &timeout)
	if err != nil {
		panic(err)
	}
}

func getDatabase(t *testing.T, config EventStoreDockerConfig, req testcontainers.ContainerRequest) *Container {
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		t.Fatalf("error when starting container: %v", err)
	}

	port, err := container.MappedPort(context.Background(), nat.Port(config.Port))

	if err != nil {
		t.Fatalf("error when looking up container mapped port %s: %v", config.Port, err)
	}

	t.Logf("[debug] container got port %s mapped to %s", config.Port, port)

	endpoint := fmt.Sprintf("localhost:%s", port.Port())

	t.Logf("[debug] endpoint is localhost:%s", port.Port())

	if !container.IsRunning() {
		t.Fatalf("failed to get a running container after many attempts")
	}

	return &Container{
		Endpoint:  endpoint,
		Container: container,
	}
}

func verifyCertificatesExist() error {
	certs := []string{
		path.Join("ca", "ca.crt"),
		path.Join("ca", "ca.key"),
		path.Join("node", "node.crt"),
		path.Join("node", "node.key"),
	}

	certsDir, err := getCertificatesDir()
	if err != nil {
		return err
	}

	for _, f := range certs {
		if _, err := os.Stat(path.Join(certsDir, f)); os.IsNotExist(err) {
			return fmt.Errorf("could not locate the certificates needed to run EventStoreDB and the tests. Please run 'docker-compose up' for generating the certificates")
		}
	}
	return nil
}

func getCertificatesDir() (string, error) {
	rootDir, err := getRootDir()
	if err != nil {
		return "", err
	}
	return path.Join(rootDir, "certs"), nil
}

func getRootDir() (string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	currentDir = strings.Replace(currentDir, "\\", "/", -1)
	return path.Clean(path.Join(currentDir, "../")), nil
}

func GetClient(t *testing.T, container *Container) *esdb.Client {
	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"
	isCluster := GetEnvOrDefault("CLUSTER", "false") == "true"

	if isCluster {
		return CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=leader&tlsverifycert=false&maxDiscoverAttempts=50&defaultDeadline=60000", t)
	} else if isInsecure {
		return createTestClient(fmt.Sprintf("esdb://%s?tls=false", container.Endpoint), container, t)
	}

	return createTestClient(fmt.Sprintf("esdb://admin:changeit@%s?tlsverifycert=false&tls=true", container.Endpoint), container, t)
}

func CreateEmptyDatabase(t *testing.T) (*Container, *esdb.Client) {
	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

	var container *Container
	var client *esdb.Client

	if GetEnvOrDefault("CLUSTER", "false") == "true" {
		client = GetClient(t, nil)
	} else {
		if isInsecure {
			t.Logf("[debug] starting insecure database container...")
		} else {
			t.Logf("[debug] starting database container...")
		}

		config, req, err := getContainerRequest()

		fmt.Println(spew.Sdump(req))

		if err != nil {
			t.Fatalf("error when constructing testcontainer request: %v", err)
		}

		container = getDatabase(t, *config, *req)
		client = GetClient(t, container)

	}

	if client != nil {
		WaitForAdminToBeAvailable(t, client)
		WaitForLeaderToBeElected(t, client)
	}

	return container, client
}

func createTestClient(conn string, container *Container, t *testing.T) *esdb.Client {
	t.Logf("[debug] connection string => %s", conn)

	config, err := esdb.ParseConnectionString(conn)
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	fmt.Println(spew.Sdump(config))

	client, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	return client
}

func WaitForAdminToBeAvailable(t *testing.T, db *esdb.Client) {
	for count := 0; count < 50; count++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Logf("[debug] checking if admin user is available...%v/50", count)

		stream, err := db.ReadStream(ctx, "$users", esdb.ReadStreamOptions{}, 1)

		if ctx.Err() != nil {
			t.Log("[debug] request timed out, retrying...")
			cancel()
			time.Sleep(1 * time.Second)
			continue
		}

		if stream != nil {
			_, err = stream.Recv()
			if err == nil {
				t.Log("[debug] admin is available!")
				cancel()
				stream.Close()
				return
			}
		}

		if err != nil {
			if esdbError, ok := esdb.FromError(err); !ok {
				// If we are in insecure mode, the $users stream is not available.
				if db.Config().DisableTLS && esdbError.Code() == esdb.ErrorCodeResourceNotFound {
					t.Log("[debug] admin is available!")
					cancel()
					stream.Close()
					return
				}

				if esdbError.Code() == esdb.ErrorCodeResourceNotFound ||
					esdbError.Code() == esdb.ErrorCodeUnauthenticated ||
					esdbError.Code() == esdb.ErrorCodeDeadlineExceeded ||
					esdbError.Code() == esdb.ErrorUnavailable {
					time.Sleep(1 * time.Second)
					t.Logf("[debug] not available retrying...")
					cancel()
					continue
				}

				t.Fatalf("unexpected error when waiting the admin account to be available: %+v", esdbError)
			}

			t.Fatalf("unexpected error when waiting the admin account to be available: %+v", err)
		}
	}

	t.Fatalf("failed to access admin account in a timely manner")
}

func WaitForLeaderToBeElected(t *testing.T, db *esdb.Client) {
	for count := 0; count < 50; count++ {
		streamID := NAME_GENERATOR.Generate()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Logf("[debug] checking if a leader has been elected...%v/50", count)

		err := db.CreatePersistentSubscription(ctx, streamID, "group", esdb.PersistentStreamSubscriptionOptions{})

		if ctx.Err() != nil {
			t.Log("[debug] request timed out, retrying...")
			cancel()
			time.Sleep(1 * time.Second)
			continue
		}

		if err == nil {
			cancel()
			t.Log("[debug] a leader has been elected!")
			return
		}

		if err != nil {
			if esdbError, ok := esdb.FromError(err); !ok {
				if esdbError.Code() == esdb.ErrorCodeNotLeader || esdbError.Code() == esdb.ErrorUnavailable || esdbError.Code() == esdb.ErrorCodeUnauthenticated {
					time.Sleep(1 * time.Second)
					t.Logf("[debug] not available retrying...")
					cancel()
					continue
				}

				t.Fatalf("unexpected error when waiting for the cluster to elect a leader: %+v", esdbError)
			}

			t.Fatalf("unexpected error when waiting for the cluster to elect a leader: %+v", err)
		}
	}

	t.Fatalf("cluster failed to elect a leader in a timely manner")
}

func CreateClient(connStr string, t *testing.T) *esdb.Client {
	config, err := esdb.ParseConnectionString(connStr)

	if err != nil {
		t.Fatalf("Error when parsin connection string: %v", err)
	}

	client, err := esdb.NewClient(config)

	if err != nil {
		t.Fatalf("Error when creating an ESDB esdb: %v", err)
	}

	return client
}
