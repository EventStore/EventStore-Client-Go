package client_test

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/ory/dockertest"
)

const EVENTSTORE_DOCKER_REPOSITORY = "docker.pkg.github.com/eventstore/eventstore-client-grpc-testdata/eventstore-client-grpc-testdata"
const EVENTSTORE_DOCKER_TAG = "20.6.0-buster-slim"
const EVENTSTORE_DOCKER_PORT = "2113"

// Container ...
type Container struct {
	Endpoint string
	Resource *dockertest.Resource
}

func (container *Container) Close() {
	container.Resource.Close()
}

func GetEmptyDatabase() *Container {
	options := &dockertest.RunOptions{
		Repository:   EVENTSTORE_DOCKER_REPOSITORY,
		Tag:          EVENTSTORE_DOCKER_TAG,
		ExposedPorts: []string{EVENTSTORE_DOCKER_PORT},
		Env:          []string{"EVENTSTORE_DEV=true"},
	}

	return getDatabase(options)
}

func GetPrePopulatedDatabase() *Container {
	options := &dockertest.RunOptions{
		Repository:   EVENTSTORE_DOCKER_REPOSITORY,
		Tag:          EVENTSTORE_DOCKER_TAG,
		ExposedPorts: []string{EVENTSTORE_DOCKER_PORT},
		Env:          []string{"EVENTSTORE_DEV=true", "EVENTSTORE_DB=/data/integration-tests", "EVENTSTORE_MEM_DB=false"},
	}
	return getDatabase(options)
}

func getDatabase(options *dockertest.RunOptions) *Container {
	pool, err := dockertest.NewPool("")

	if err != nil {
		log.Fatalf("Could not connect to docker. Reason: %v", err)
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		log.Fatalf("Could not start resource. Reason: %v", err)
	}

	endpoint := fmt.Sprintf("localhost:%s", resource.GetPort("2113/tcp"))

	// Disable certificate verification
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	err = pool.Retry(func() error {
		healthCheckEndpoint := fmt.Sprintf("https://%s/health/live", endpoint)
		_, err := http.Get(healthCheckEndpoint)
		return err
	})

	if err != nil {
		log.Fatalf("HealthCheck failed. Reason: %v", err)
	}

	return &Container{
		Endpoint: endpoint,
		Resource: resource,
	}
}

func CreateTestClient(container *Container, t *testing.T) *client.Client {
	config := client.NewConfiguration()
	config.Address = container.Endpoint
	config.Username = "admin"
	config.Password = "changeit"
	config.SkipCertificateVerification = true

	client, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}
	err = client.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}
	return client
}
