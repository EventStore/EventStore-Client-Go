package esdb_test

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
)

func TLSTests(t *testing.T, emptyDBContainer *Container) {
	t.Run("TLSTests", func(t *testing.T) {
		t.Run("Default", testTLSDefaults(emptyDBContainer))
		t.Run("DefaultsWithCertificate", testTLSDefaultsWithCertificate(emptyDBContainer))
		t.Run("WithoutCertificateAndVerify", testTLSWithoutCertificateAndVerify(emptyDBContainer))
		t.Run("testTLSWithoutCertificate(", testTLSWithoutCertificate(emptyDBContainer))
		t.Run("WithCertificate", testTLSWithCertificate(emptyDBContainer))
		t.Run("WithCertificateFromAbsoluteFile", testTLSWithCertificateFromAbsoluteFile(emptyDBContainer))
		t.Run("WithCertificateFromRelativeFile", testTLSWithCertificateFromRelativeFile(emptyDBContainer))
		t.Run("WithInvalidCertificate", testTLSWithInvalidCertificate(emptyDBContainer))
	})
}

func testTLSDefaults(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		c, err := esdb.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}
		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}

		_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to verify certificate")
	}
}

func testTLSDefaultsWithCertificate(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		b, err := os.ReadFile("../certs/node/node.crt")
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

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		stream, err := c.ReadAll(context.Background(), opts, numberOfEvents)
		require.NoError(t, err)
		defer stream.Close()
		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func testTLSWithoutCertificateAndVerify(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=false", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		c, err := esdb.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		stream, err := c.ReadAll(context.Background(), opts, numberOfEvents)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func testTLSWithoutCertificate(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		c, err := esdb.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to verify certificate")
	}
}

func testTLSWithCertificate(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		b, err := os.ReadFile("../certs/node/node.crt")
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

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		stream, err := c.ReadAll(context.Background(), opts, numberOfEvents)
		require.NoError(t, err)
		defer stream.Close()
		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func testTLSWithCertificateFromAbsoluteFile(container *Container) TestCall {
	return func(t *testing.T) {
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

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		stream, err := c.ReadAll(context.Background(), opts, numberOfEvents)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func testTLSWithCertificateFromRelativeFile(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true&tlsCAFile=../certs/node/node.crt", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		c, err := esdb.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}

		defer c.Close()

		WaitForAdminToBeAvailable(t, c)
		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		stream, err := c.ReadAll(context.Background(), opts, numberOfEvents)
		require.NoError(t, err)
		defer stream.Close()
		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func testTLSWithInvalidCertificate(container *Container) TestCall {
	return func(t *testing.T) {
		config, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://admin:changeit@%s?tls=true&tlsverifycert=true", container.Endpoint))
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		b, err := os.ReadFile("../certs/untrusted-ca/ca.crt")
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

		defer c.Close()

		numberOfEventsToRead := 1
		numberOfEvents := uint64(numberOfEventsToRead)
		opts := esdb.ReadAllOptions{
			From:           esdb.Start{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}
		_, err = c.ReadAll(context.Background(), opts, numberOfEvents)
		esdbErr, ok := esdb.FromError(err)
		require.False(t, ok)
		require.NotNil(t, esdbErr)
		assert.Contains(t, esdbErr.Error(), "certificate signed by unknown authority")
	}
}
