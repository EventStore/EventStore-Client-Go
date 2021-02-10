package client_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/position"
)

func TestTLSWithoutCertificateAndVerify(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	err = c.Connect()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	_, err = c.ReadAllEvents(context.Background(), direction.Backwards, position.StartPosition, numberOfEvents, true)
	require.NoError(t, err)
}

func TestTLSWithoutCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	err = c.Connect()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	_, err = c.ReadAllEvents(context.Background(), direction.Backwards, position.StartPosition, numberOfEvents, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func TestTLSWithCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	b, err := ioutil.ReadFile("../certs/node/node.crt")
	if err != nil {
		t.Fatalf("failed to read node certificate ../certs/node/node.crt: %s", err.Error())
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		t.Fatalf("failed to append node certificates: %s", err.Error())
	}
	config.RootCAs = cp

	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	err = c.Connect()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	_, err = c.ReadAllEvents(context.Background(), direction.Backwards, position.StartPosition, numberOfEvents, true)
	require.NoError(t, err)
}

func TestTLSWithInvalidCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	b, err := ioutil.ReadFile("../certs/untrusted-ca/ca.crt")
	if err != nil {
		t.Fatalf("failed to read node certificate ../certs/untrusted-ca/ca.crt: %s", err.Error())
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		t.Fatalf("failed to append node certificates: %s", err.Error())
	}
	config.RootCAs = cp

	c, err := client.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	err = c.Connect()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	_, err = c.ReadAllEvents(context.Background(), direction.Backwards, position.StartPosition, numberOfEvents, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}
