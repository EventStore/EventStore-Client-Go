package esdb_test

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

func TestTLSDefaults(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}

	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func TestTLSDefaultsWithCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
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

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.True(t, errors.Is(err, io.EOF))
}

func TestTLSWithoutCertificateAndVerify(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.True(t, errors.Is(err, io.EOF))
}

func TestTLSWithoutCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func TestTLSWithCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
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

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.True(t, errors.Is(err, io.EOF))
}

func TestTLSWithCertificateFromAbsoluteFile(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	absPath, err := filepath.Abs("../certs/node/node.crt")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	s := fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=%s", container.Endpoint, absPath)
	config, err := esdb.ParseConnectionString(s)
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.True(t, errors.Is(err, io.EOF))
}

func TestTLSWithCertificateFromRelativeFile(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=../certs/node/node.crt", container.Endpoint))
	if err != nil {
		t.Fatalf("Unexpected configuration error: %s", err.Error())
	}

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.True(t, errors.Is(err, io.EOF))
}

func TestTLSWithInvalidCertificate(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
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

	c, err := esdb.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	numberOfEventsToRead := 1
	numberOfEvents := uint64(numberOfEventsToRead)
	opts := esdb.ReadAllOptions{
		From:           esdb.Start{},
		Direction:      esdb.Backwards,
		ResolveLinkTos: true,
	}
	_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}
