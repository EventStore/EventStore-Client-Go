package client_test

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionStringWithNoSchema(t *testing.T) {
	_, err := client.ParseConnectionString(":so/mething/random")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scheme is missing")
}

func TestConnectionStringWithInvalidScheme(t *testing.T) {
	_, err := client.ParseConnectionString("esdbwrong://")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid scheme")
}

func TestConnectionStringWithInvalidUserCredentials(t *testing.T) {
	_, err := client.ParseConnectionString("esdb://:pass@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is empty")

	_, err = client.ParseConnectionString("esdb://user@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password is not set")

	_, err = client.ParseConnectionString("esdb://user:@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password is empty")
}
