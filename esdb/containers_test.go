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

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
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
	DEFAULT_EVENTSTORE_DOCKER_TAG        = "ci"
	DEFAULT_EVENTSTORE_DOCKER_PORT       = "2113"
)

var defaultEventStoreDockerConfig = EventStoreDockerConfig{
	Repository: DEFAULT_EVENTSTORE_DOCKER_REPOSITORY,
	Tag:        DEFAULT_EVENTSTORE_DOCKER_TAG,
	Port:       DEFAULT_EVENTSTORE_DOCKER_PORT,
}

func readEnvironmentVariables(config EventStoreDockerConfig) EventStoreDockerConfig {
	if value, exists := os.LookupEnv(EVENTSTORE_DOCKER_REPOSITORY_ENV); exists {
		config.Repository = value
	}

	if value, exists := os.LookupEnv(EVENTSTORE_DOCKER_TAG_ENV); exists {
		config.Tag = value
	}

	if value, exists := os.LookupEnv(EVENTSTORE_DOCKER_PORT_ENV); exists {
		config.Port = value
	}

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
	if value, exists := os.LookupEnv(EVENTSTORE_DOCKER_TAG_ENV); exists {
		if value == "ci" {
			return false
		}

		parts := strings.Split(value, "-")
		versionNumbers := strings.Split(parts[0], ".")
		maj, err := strconv.Atoi(versionNumbers[0])

		if err != nil {
			panic(err)
		}

		min, err := strconv.Atoi(versionNumbers[1])

		if err != nil {
			panic(err)
		}

		patch, err := strconv.Atoi(versionNumbers[2])

		if err != nil {
			panic(err)
		}

		version := ESDBVersion{
			Maj:   maj,
			Min:   min,
			Patch: patch,
		}
		return predicate(version)
	}

	return false
}

func IsESDBVersion20() bool {
	return IsESDB_Version(func(version ESDBVersion) bool {
		return version.Maj < 21
	})
}

func getDockerOptions() *dockertest.RunOptions {
	config := readEnvironmentVariables(defaultEventStoreDockerConfig)
	return &dockertest.RunOptions{
		Repository:   config.Repository,
		Tag:          config.Tag,
		ExposedPorts: []string{config.Port},
	}
}

func (container *Container) Close() {
	err := container.Resource.Close()
	if err != nil {
		panic(err)
	}
}

func GetEmptyDatabase(t *testing.T) *Container {
	options := getDockerOptions()
	return getDatabase(t, options)
}

func GetPrePopulatedDatabase(t *testing.T) *Container {
	options := getDockerOptions()
	options.Env = []string{
		"EVENTSTORE_DB=/data/integration-tests",
		"EVENTSTORE_MEM_DB=false",
	}
	return getDatabase(t, options)
}

// TODO - Keep retrying when the healthcheck failed. We should try creating a new container instead of failing the test.
func getDatabase(t *testing.T, options *dockertest.RunOptions) *Container {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker. Reason: %v", err)
	}

	err = setTLSContext(options)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Starting docker container...")

	var resource *dockertest.Resource
	var endpoint string
	retries := 0
	retryLimit := 5

	for retries < retryLimit {
		retries += 1
		resource, err = pool.RunWithOptions(options)
		if err != nil {
			t.Fatalf("Could not start resource. Reason: %v", err)
		}

		fmt.Printf("Started container with id: %v, name: %s\n",
			resource.Container.ID,
			resource.Container.Name)

		endpoint = fmt.Sprintf("localhost:%s", resource.GetPort("2113/tcp"))

		// Disable certificate verification
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		err = pool.Retry(func() error {
			if resource != nil && resource.Container != nil {
				containerInfo, containerError := pool.Client.InspectContainer(resource.Container.ID)
				if containerError == nil && containerInfo.State.Running == false {
					return fmt.Errorf("unexpected exit of container check the container logs for more information, container ID: %v", resource.Container.ID)
				}
			}

			healthCheckEndpoint := fmt.Sprintf("https://%s/health/alive", endpoint)
			_, err := http.Get(healthCheckEndpoint)
			return err
		})

		if err != nil {
			log.Printf("[debug] healthCheck failed. Reason: %v\n", err)

			closeErr := resource.Close()

			if closeErr != nil && retries >= retryLimit {
				t.Fatalf("Failed to closeConnection docker resource. Reason: %v", err)
			}

			if retries < retryLimit {
				log.Printf("[debug] heatlhCheck failed retrying...%v/%v", retries, retryLimit)
				continue
			}

			t.Fatal("[debug] stopping docker resource")
		} else {
			log.Print("[debug] healthCheck succeeded!")
			break
		}
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

func CreateTestClient(container *Container, t *testing.T) *esdb.Client {
	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tlsverifycert=false", container.Endpoint))
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
			if err.Error() == "not leader exception" {
				time.Sleep(500 * time.Microsecond)
				t.Logf("[debug] not available retrying...")
				cancel()
				continue
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
