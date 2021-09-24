package client_test

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ory/dockertest/v3"
	"github.com/pivonroll/EventStore-Client-Go/client"
	"github.com/stretchr/testify/require"
)

type EventStoreEnvironmentVariable string

const (
	EVENTSTORE_DOCKER_REPOSITORY_ENV    EventStoreEnvironmentVariable = "EVENTSTORE_DOCKER_REPOSITORY"
	EVENTSTORE_DOCKER_TAG_ENV           EventStoreEnvironmentVariable = "EVENTSTORE_DOCKER_TAG"
	EVENTSTORE_DOCKER_PORT_ENV          EventStoreEnvironmentVariable = "EVENTSTORE_DOCKER_PORT"
	EVENTSTORE_MAX_APPEND_SIZE_IN_BYTES EventStoreEnvironmentVariable = "EVENTSTORE_MAX_APPEND_SIZE"
)

func createEventStoreEnvironmentVar(variableName EventStoreEnvironmentVariable, value string) string {
	return fmt.Sprintf("%s=%s", variableName, value)
}

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
	DEFAULT_EVENTSTORE_DOCKER_REPOSITORY = "docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata"
	DEFAULT_EVENTSTORE_DOCKER_TAG        = "21.6.0-buster-slim"
	DEFAULT_EVENTSTORE_DOCKER_PORT       = "2113"
)

var defaultEventStoreDockerConfig = EventStoreDockerConfig{
	Repository: DEFAULT_EVENTSTORE_DOCKER_REPOSITORY,
	Tag:        DEFAULT_EVENTSTORE_DOCKER_TAG,
	Port:       DEFAULT_EVENTSTORE_DOCKER_PORT,
}

func readEnvironmentVariables(config EventStoreDockerConfig) EventStoreDockerConfig {
	if value, exists := os.LookupEnv(string(EVENTSTORE_DOCKER_REPOSITORY_ENV)); exists {
		config.Repository = value
	}

	if value, exists := os.LookupEnv(string(EVENTSTORE_DOCKER_TAG_ENV)); exists {
		config.Tag = value
	}

	if value, exists := os.LookupEnv(string(EVENTSTORE_DOCKER_PORT_ENV)); exists {
		config.Port = value
	}

	fmt.Println(spew.Sdump(config))
	return config
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

func getEmptyDatabase(environmentVariables ...string) *Container {
	options := getDockerOptions()
	options.Env = append(options.Env, environmentVariables...)
	return getDatabase(options)
}

func getPrePopulatedDatabase() *Container {
	options := getDockerOptions()
	options.Env = []string{
		"EVENTSTORE_DB=/data/integration-tests",
		"EVENTSTORE_MEM_DB=false",
	}
	return getDatabase(options)
}

func getDatabase(options *dockertest.RunOptions) *Container {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker. Reason: %v", err)
	}

	err = setTLSContext(options)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Environment Variables:\n", strings.Join(options.Env, "\n"))
	fmt.Println("\nStarting docker container...")

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource. Reason: %v", err)
	}

	fmt.Printf("Started container with id: %v, name: %s\n",
		resource.Container.ID,
		resource.Container.Name)

	endpoint := fmt.Sprintf("localhost:%s", resource.GetPort("2113/tcp"))

	// Disable certificate verification
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	err = pool.Retry(func() error {
		if resource != nil && resource.Container != nil {
			containerInfo, containerError := pool.Client.InspectContainer(resource.Container.ID)
			if containerError == nil && containerInfo.State.Running == false {
				return fmt.Errorf("unexpected exit of container check the container logs for more information, container ID: %v", resource.Container.ID)
			}
		}

		healthCheckEndpoint := fmt.Sprintf("https://%s", endpoint)
		_, err := http.Get(healthCheckEndpoint)
		return err
	})

	if err != nil {
		log.Printf("HealthCheck failed. Reason: %v\n", err)

		closeErr := resource.Close()

		if closeErr != nil {
			log.Fatalf("Failed to close docker resource. Reason: %v", err)
		}
		log.Fatalln("Stopping docker resource")
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

func createClientConnectedToContainer(container *Container, t *testing.T) *client.Client {
	clientInstance := createClientConnectedToURI(
		fmt.Sprintf("esdb://admin:changeit@%s?tlsverifycert=false", container.Endpoint), t)
	return clientInstance
}

func createClientConnectedToURI(connStr string, t *testing.T) *client.Client {
	config, err := client.ParseConnectionString(connStr)
	if err != nil {
		t.Fatalf("Error when parsin connection string: %v", err)
	}

	clientInstance, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Error when creating an ESDB client: %v", err)
	}

	return clientInstance
}

type closeClientInstanceFunc func()

func initializeContainerAndClient(t *testing.T,
	environmentVariables ...string) (*Container, *client.Client, closeClientInstanceFunc) {
	container := getEmptyDatabase(environmentVariables...)
	clientInstance := createClientConnectedToContainer(container, t)
	closeClientInstance := func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}
	return container, clientInstance, closeClientInstance
}
