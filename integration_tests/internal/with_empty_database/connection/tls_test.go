package connection_integration_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"path/filepath"
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

	_, err := eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func testGetCertificatePoolForFile(t *testing.T, filePath string) *x509.CertPool {
	b, err := ioutil.ReadFile(filePath)
	require.NoErrorf(t, err, fmt.Sprintf("failed to read node certificate %s:", filePath))

	certificatePool := x509.NewCertPool()
	require.True(t, certificatePool.AppendCertsFromPEM(b))

	return certificatePool
}

func TestTLSDefaultsWithCertificate(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s",
		container.Endpoint))
	require.NoError(t, err)

	certificatePool := testGetCertificatePoolForFile(t, joinRootPathAndFilePath("certs/node/node.crt"))

	config.RootCAs = certificatePool

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		false)
	require.NoError(t, err)
}

func TestTLSWithoutCertificateAndVerify(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(
		fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
	require.NoError(t, err)

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		false)
	require.NoError(t, err)
}

func TestTLSWithoutCertificate(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	require.NoError(t, err)

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		false)

	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate signed by unknown authority")
}

func TestTLSWithCertificate(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	require.NoError(t, err)

	certificatePool := testGetCertificatePoolForFile(t, joinRootPathAndFilePath("certs/node/node.crt"))

	config.RootCAs = certificatePool

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		true)
	require.NoError(t, err)
}

func TestTLSWithCertificateFromAbsoluteFile(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	absPath, err := filepath.Abs(joinRootPathAndFilePath("certs/node/node.crt"))
	require.NoError(t, err)

	s := fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=%s", container.Endpoint, absPath)
	config, err := client.ParseConnectionString(s)
	require.NoError(t, err)

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		true)
	require.NoError(t, err)
}

func TestTLSWithCertificateFromRelativeFile(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(
		fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=%s",
			container.Endpoint, joinRootPathAndFilePath("certs/node/node.crt")))
	require.NoError(t, err)

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		true)
	require.NoError(t, err)
}

func TestTLSWithInvalidCertificate(t *testing.T) {
	container := test_utils.StartEventStoreInDockerContainer(nil)
	defer container.Close()

	config, err := client.ParseConnectionString(
		fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
	require.NoError(t, err)

	certificatePool := testGetCertificatePoolForFile(t, joinRootPathAndFilePath("certs/untrusted-ca/ca.crt"))
	config.RootCAs = certificatePool

	grpcClient := connection.NewGrpcClient(*config)
	eventStreamsClient := initializeEventStreamsWithGrpcClient(grpcClient)

	_, err = eventStreamsClient.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionBackward,
		event_streams.ReadPositionAllStart{},
		1,
		true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")
}
