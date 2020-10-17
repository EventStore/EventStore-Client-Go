package client_test

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionStringWithNoSchema(t *testing.T) {
	config, err := client.ParseConnectionString(":so/mething/random")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "scheme is missing")
}

func TestConnectionStringWithInvalidScheme(t *testing.T) {
	config, err := client.ParseConnectionString("esdbwrong://")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid scheme")
}

func TestConnectionStringWithInvalidUserCredentials(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://:pass@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is empty")

	config, err = client.ParseConnectionString("esdb://user@127.0.0.1/")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "password is not set")

	config, err = client.ParseConnectionString("esdb://user:@127.0.0.1/")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "password is empty")
}

func TestConnectionStringWithInvalidHost(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:1234,127.0.0.2:abc,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc:def")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:abc:def")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@localhost:1234,127.0.0.2:abc:def,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid port")

	config, err = client.ParseConnectionString("esdb://user:pass@localhost:1234,,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")
}

func TestConnectionStringWithEmptyPath(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1:1234")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)
}

func TestConnectionStringWithNonEmptyPath(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/hello?maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")
}

func TestConnectionStringWithDuplicateKey(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&MaxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=10&gossipTimeout=30")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")
}

func TestConnectionStringWithInvalidSettings(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?unknown=1234")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "No value specified for")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&hello=test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?discoveryInterval=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=defg")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?tlsVerifyCert=truee")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be either true or false")

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=blabla")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")
}

func TestConnectionStringWithDifferentNodePreferences(t *testing.T) {
	config, err := client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=leader")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Leader, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=Follower")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Follower, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=rAndom")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_Random, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=ReadOnlyReplica")
	assert.NoError(t, err)
	assert.Equal(t, client.NodePreference_ReadOnlyReplica, config.NodePreference)

	config, err = client.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=invalid")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")
}