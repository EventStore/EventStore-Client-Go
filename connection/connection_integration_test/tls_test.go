package connection_integration_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/client"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTLSDefaults(t *testing.T) {
	_, eventStreamsClient, closeFunc := initializeContainerAndClientWithTLS(t, nil)
	defer closeFunc()

	_, err := eventStreamsClient.ReadAllEvents(context.Background(),
		event_streams.ReadRequestDirectionBackward,
		event_streams.ReadRequestOptionsAllStartPosition{},
		1,
		false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func testGetCertificatePoolForFile(t *testing.T, filePath string) *x509.CertPool {
	b, err := ioutil.ReadFile(filePath)
	require.NoErrorf(t, err, fmt.Sprintf("failed to read node certificate %s:", filePath))

	certificatePool := x509.NewCertPool()
	require.True(t, certificatePool.AppendCertsFromPEM(b))

	return certificatePool
}

func TestTLSDefaultsWithCertificate(t *testing.T) {
	container := test_utils.CreateDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s",
		container.Endpoint))
	require.NoError(t, err)

	certificatePool := testGetCertificatePoolForFile(t, "../../certs/node/node.crt")

	config.RootCAs = certificatePool

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadAllEvents(context.Background(),
		event_streams.ReadRequestDirectionBackward,
		event_streams.ReadRequestOptionsAllStartPosition{},
		1,
		false)
	require.NoError(t, err)
}

//
//func TestTLSWithoutCertificateAndVerify(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithoutCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.Error(t, err)
//	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
//}
//
//func TestTLSWithCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	b, err := ioutil.ReadFile("../certs/node/node.crt")
//	if err != nil {
//		t.Fatalf("failed to read node certificate ../certs/node/node.crt: %s", err.Error())
//	}
//	cp := x509.NewCertPool()
//	if !cp.AppendCertsFromPEM(b) {
//		t.Fatalf("failed to append node certificates: %s", err.Error())
//	}
//	config.RootCAs = cp
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithCertificateFromAbsoluteFile(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	absPath, err := filepath.Abs("../certs/node/node.crt")
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	s := fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=%s", container.Endpoint, absPath)
//	config, err := client.ParseConnectionString(s)
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithCertificateFromRelativeFile(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=../certs/node/node.crt", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.NoError(t, err)
//}
//
//func TestTLSWithInvalidCertificate(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	b, err := ioutil.ReadFile("../certs/untrusted-ca/ca.crt")
//	if err != nil {
//		t.Fatalf("failed to read node certificate ../certs/untrusted-ca/ca.crt: %s", err.Error())
//	}
//	cp := x509.NewCertPool()
//	if !cp.AppendCertsFromPEM(b) {
//		t.Fatalf("failed to append node certificates: %s", err.Error())
//	}
//	config.RootCAs = cp
//
//	c, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected error: %s", err.Error())
//	}
//
//	numberOfEventsToRead := 1
//	numberOfEvents := uint64(numberOfEventsToRead)
//	_, err = c.ReadAllEvents_OLD(context.Background(), direction.Backwards, stream_position.Start{}, numberOfEvents, true)
//	require.Error(t, err)
//	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
//}
