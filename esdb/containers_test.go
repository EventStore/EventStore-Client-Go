package esdb_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/davecgh/go-spew/spew"
	"github.com/goombaio/namegenerator"
	"github.com/ory/dockertest/v3"
)

const (
	EVENTSTORE_DOCKER_REPOSITORY_ENV = "EVENTSTORE_DOCKER_REPOSITORY"
	EVENTSTORE_DOCKER_TAG_ENV        = "EVENTSTORE_DOCKER_TAG_ENV"
	EVENTSTORE_DOCKER_PORT_ENV       = "EVENTSTORE_DOCKER_PORT"
)

var (
	NAME_GENERATOR = namegenerator.NewNameGenerator(0)
)

// Container ...
type Container struct {
	Endpoint string
	Resource *dockertest.Resource
}

type EventStoreDockerConfig struct {
	Repository string
	Tag        string
	Port       string
}

const (
	DEFAULT_EVENTSTORE_DOCKER_REPOSITORY = "ghcr.io/eventstore/testdata"
	DEFAULT_EVENTSTORE_DOCKER_TAG        = "latest"
	DEFAULT_EVENTSTORE_DOCKER_PORT       = "2113"
)

var defaultEventStoreDockerConfig = EventStoreDockerConfig{
	Repository: DEFAULT_EVENTSTORE_DOCKER_REPOSITORY,
	Tag:        DEFAULT_EVENTSTORE_DOCKER_TAG,
	Port:       DEFAULT_EVENTSTORE_DOCKER_PORT,
}

func GetEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func readEnvironmentVariables(config EventStoreDockerConfig) EventStoreDockerConfig {
	config.Repository = GetEnvOrDefault(EVENTSTORE_DOCKER_REPOSITORY_ENV, config.Repository)
	config.Tag = GetEnvOrDefault(EVENTSTORE_DOCKER_TAG_ENV, config.Tag)
	config.Port = GetEnvOrDefault(EVENTSTORE_DOCKER_PORT_ENV, config.Port)

	fmt.Println(spew.Sdump(config))
	return config
}

type ESDBVersion struct {
	Maj   int
	Min   int
	Patch int
}

type VersionPredicateFn = func(ESDBVersion) bool

func IsESDB_Version(predicate VersionPredicateFn) bool {
	value, exists := os.LookupEnv(EVENTSTORE_DOCKER_TAG_ENV)
	if !exists || value == "ci" {
		return false
	}

	parts := strings.Split(value, "-")
	versionNumbers := strings.Split(parts[0], ".")

	version := ESDBVersion{
		Maj:   mustConvertToInt(versionNumbers[0]),
		Min:   mustConvertToInt(versionNumbers[1]),
		Patch: mustConvertToInt(versionNumbers[2]),
	}

	return predicate(version)
}

func mustConvertToInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return val
}

func IsESDBVersion20() bool {
	return IsESDB_Version(func(version ESDBVersion) bool {
		return version.Maj < 21
	})
}

func getDockerOptions() *dockertest.RunOptions {
	config := readEnvironmentVariables(defaultEventStoreDockerConfig)

	var envs []string
	if isInsecure, exists := os.LookupEnv("EVENTSTORE_INSECURE"); exists {
		envs = append(envs, fmt.Sprintf("EVENTSTORE_INSECURE=%s", isInsecure))
	}

	if len(envs) == 0 {
		envs = append(envs, fmt.Sprintf("EVENTSTORE_INSECURE=%s", "true"))
	}

	return &dockertest.RunOptions{
		Repository:   config.Repository,
		Tag:          config.Tag,
		ExposedPorts: []string{config.Port},
		Env:          envs,
	}
}

func (container *Container) Close() {
	err := container.Resource.Close()
	if err != nil {
		panic(err)
	}
}

func getDatabase(t *testing.T, options *dockertest.RunOptions) *Container {
	const maxContainerRetries = 5
	const healthCheckRetries = 10

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker. Reason: %v", err)
	}

	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"
	if !isInsecure {
		err = setTLSContext(options)
		if err != nil {
			t.Fatal(err)
		}
	}

	var resource *dockertest.Resource
	var endpoint string

	for containerRetry := 0; containerRetry < maxContainerRetries; containerRetry++ {
		resource, err = pool.RunWithOptions(options)
		if err != nil {
			t.Fatalf("Could not start resource. Reason: %v", err)
		}

		fmt.Printf("Started container with ID: %v, name: %s\n", resource.Container.ID, resource.Container.Name)
		endpoint = fmt.Sprintf("localhost:%s", resource.GetPort("2113/tcp"))

		// Disable certificate verification
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		healthCheckSuccess := false

		for healthRetry := 0; healthRetry < healthCheckRetries; healthRetry++ {
			scheme := "https"
			if isInsecure {
				scheme = "http"
			}

			healthCheckEndpoint := fmt.Sprintf("%s://%s/health/alive", scheme, endpoint)
			_, err := http.Get(healthCheckEndpoint)

			if err == nil {
				healthCheckSuccess = true
				break
			}

			time.Sleep(2 * time.Second)
		}

		if healthCheckSuccess {
			break
		} else {
			log.Printf("[debug] Health check failed for container %d/%d. Creating a new one...", containerRetry+1, maxContainerRetries)
			resource.Close()
		}
	}

	if !resource.Container.State.Running {
		t.Fatalf("Failed to get a running container after %d attempts", maxContainerRetries)
	}

	return &Container{
		Endpoint: endpoint,
		Resource: resource,
	}
}

func setTLSContext(options *dockertest.RunOptions) error {
	err := verifyCertificatesExist()
	if err != nil {
		return err
	}

	options.Env = append(options.Env, []string{
		"EVENTSTORE_CERTIFICATE_FILE=/etc/eventstore/certs/node/node.crt",
		"EVENTSTORE_CERTIFICATE_PRIVATE_KEY_FILE=/etc/eventstore/certs/node/node.key",
		"EVENTSTORE_TRUSTED_ROOT_CERTIFICATES_PATH=/etc/eventstore/certs/ca",
	}...)

	certsDir, err := getCertificatesDir()
	if err != nil {
		return err
	}
	options.Mounts = []string{
		fmt.Sprintf("%v:/etc/eventstore/certs", certsDir),
	}
	return nil
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
		return CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=leader&tlsverifycert=false", t)
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
		container = nil
		client = GetClient(t, container)
	} else {
		if isInsecure {
			t.Log("[debug] starting insecure database container...")
		} else {
			t.Log("[debug] starting empty database container...")
		}

		options := getDockerOptions()
		container = getDatabase(t, options)
		client = GetClient(t, container)
	}

	WaitForAdminToBeAvailable(t, client)
	WaitForLeaderToBeElected(t, client)

	return container, client
}

func CreatePopulatedDatabase(t *testing.T) (*Container, *esdb.Client) {
	isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

	if GetEnvOrDefault("CLUSTER", "false") == "true" {
		return nil, nil
	} else {
		if isInsecure {
			t.Log("[debug] starting prepopulated insecure database container...")
		} else {
			t.Log("[debug] starting prepopulated database container...")
		}

		options := getDockerOptions()
		options.Env = append(options.Env, "EVENTSTORE_DB=/data/integration-tests", "EVENTSTORE_MEM_DB=false")

		container := getDatabase(t, options)
		client := GetClient(t, container)

		WaitForAdminToBeAvailable(t, client)

		return container, client
	}

}

func createTestClient(conn string, container *Container, t *testing.T) *esdb.Client {
	config, err := esdb.ParseConnectionString(conn)
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	client, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}

	return client
}

func WaitForAdminToBeAvailable(t *testing.T, db *esdb.Client) {
	count := 0

	for count < 50 {
		count += 1

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		t.Logf("[debug] checking if admin user is available...%v/50", count)

		stream, err := db.ReadStream(ctx, "$users", esdb.ReadStreamOptions{}, 1)

		if ctx.Err() != nil {
			t.Log("[debug] request timed out, retrying...")
			cancel()
			continue
		}

		if err != nil {
			if esdbError, ok := esdb.FromError(err); !ok {
				if esdbError.Code() == esdb.ErrorCodeResourceNotFound || esdbError.Code() == esdb.ErrorCodeUnauthenticated {
					time.Sleep(500 * time.Microsecond)
					t.Logf("[debug] not available retrying...")
					cancel()
					continue
				}

				t.Fatalf("unexpected error when waiting the admin account to be available: %+v", err)
			}
		}

		cancel()
		stream.Close()
		t.Log("[debug] admin is available!")
		break
	}
}

func WaitForLeaderToBeElected(t *testing.T, db *esdb.Client) {
	count := 0
	streamID := NAME_GENERATOR.Generate()

	for count < 50 {
		count += 1

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Logf("[debug] checking if a leader has been elected...%v/50", count)

		err := db.CreatePersistentSubscription(ctx, streamID, "group", esdb.PersistentStreamSubscriptionOptions{})

		if ctx.Err() != nil {
			t.Log("[debug] request timed out, retrying...")
			cancel()
			continue
		}

		if err != nil {
			if esdbError, ok := esdb.FromError(err); !ok {
				if esdbError.Code() == esdb.ErrorCodeNotLeader || esdbError.Code() == esdb.ErrorUnavailable {
					time.Sleep(500 * time.Microsecond)
					t.Logf("[debug] not available retrying...")
					cancel()
					continue
				}
			}

			panic(err)
		}

		cancel()
		t.Log("[debug] a leader has been elected!")
		break
	}
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
