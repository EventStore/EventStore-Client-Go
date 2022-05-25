package esdb_test

import (
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionStringDefaults(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://localhost")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:2113", config.Address)
	assert.Equal(t, 100, config.DiscoveryInterval)
	assert.Equal(t, 5, config.GossipTimeout)
	assert.Equal(t, 10, config.MaxDiscoverAttempts)
	assert.Equal(t, 10*time.Second, config.KeepAliveInterval)
	assert.Equal(t, 10*time.Second, config.KeepAliveTimeout)
	assert.Equal(t, esdb.NodePreferenceLeader, config.NodePreference)
}

func TestConnectionStringWithNoSchema(t *testing.T) {
	config, err := esdb.ParseConnectionString(":so/mething/random")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "scheme is missing")
}

func TestConnectionStringWithInvalidScheme(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdbwrong://")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "invalid scheme")
}

func TestConnectionStringWithInvalidUserCredentials(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user credentials are invalid")

	config, err = esdb.ParseConnectionString("esdb://us:er:pa:ss@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user credentials are invalid")

	config, err = esdb.ParseConnectionString("esdb://:pass@127.0.0.1/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username is empty")

	config, err = esdb.ParseConnectionString("esdb://user:@127.0.0.1/")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "password is empty")
}

func TestConnectionStringWithInvalidHost(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")

	config, err = esdb.ParseConnectionString("esdb://user:pass@")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1:abc")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "expecting an integer")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1:1234,127.0.0.2:abc,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "expecting an integer")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1:abc:def")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "too many colons")

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:1234,127.0.0.2:abc:def,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "too many colons")

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:1234,,127.0.0.3:4321")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "empty host")
}

func TestConnectionStringWithEmptyPath(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1:1234")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=10")
	assert.Nil(t, err)
	assert.NotNil(t, config)
}

func TestConnectionStringWithNonEmptyPath(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/hello?maxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "path must be either an empty string or a forward slash")
}

func TestConnectionStringWithDuplicateKey(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&MaxDiscoverAttempts=10")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=10&gossipTimeout=30")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "duplicate key/value pair")
}

func TestConnectionStringWithoutSettings(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://localhost")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:2113", config.Address)

	config, err = esdb.ParseConnectionString("esdb://localhost:2114")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:2114", config.Address)

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:2114")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:2114", config.Address)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:2114/")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:2114", config.Address)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/")
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?")
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
}

func TestConnectionStringWithInvalidKeyValuePair(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=12=34")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid key/value pair specified")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts1234")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid key/value pair specified")
}

func TestConnectionStringWithInvalidSettings(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?unknown=1234")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "No value specified for")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=1234&hello=test")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Unknown setting")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?discoveryInterval=abcd")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?gossipTimeout=defg")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be an integer value")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?tlsVerifyCert=truee")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "must be either true or false")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=blabla")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1?/")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.NotNil(t, err.Error())
}

func TestConnectionStringWithDifferentNodePreferences(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=leader")
	assert.NoError(t, err)
	assert.Equal(t, esdb.NodePreferenceLeader, config.NodePreference)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=Follower")
	assert.NoError(t, err)
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=rAndom")
	assert.NoError(t, err)
	assert.Equal(t, esdb.NodePreferenceRandom, config.NodePreference)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=ReadOnlyReplica")
	assert.NoError(t, err)
	assert.Equal(t, esdb.NodePreferenceReadOnlyReplica, config.NodePreference)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?nodePreference=invalid")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid NodePreference")
}

func TestConnectionStringWithValidSingleNodeConnectionString(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@localhost:2114?tlsVerifyCert=false")
	require.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "localhost:2114", config.Address)
	assert.Equal(t, true, config.SkipCertificateVerification)
	assert.Equal(t, false, config.DnsDiscover)

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:2114?tls=false")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "localhost:2114", config.Address)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?maxDiscoverAttempts=13&DiscoveryInterval=37&gossipTimeout=33&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=faLse")
	require.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "127.0.0.1:2113", config.Address)
	assert.Empty(t, config.GossipSeeds)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, 33, config.GossipTimeout)
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)
	assert.Equal(t, true, config.SkipCertificateVerification)

	config, err = esdb.ParseConnectionString("esdb://hostname:4321/?tls=fAlse")
	require.NoError(t, err)
	assert.Empty(t, config.Username)
	assert.Empty(t, config.Password)
	assert.Equal(t, "hostname:4321", config.Address)
	assert.Empty(t, config.GossipSeeds)
	assert.Equal(t, esdb.NodePreferenceLeader, config.NodePreference)
	assert.Equal(t, true, config.DisableTLS)
	assert.Equal(t, false, config.SkipCertificateVerification)

	config, err = esdb.ParseConnectionString("esdb+discover://user:pass@host?nodePreference=follower&tlsVerifyCert=false")
	require.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "host:2113", config.Address)
	assert.Empty(t, config.GossipSeeds)
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)
	assert.Equal(t, false, config.DisableTLS)
	assert.Equal(t, true, config.SkipCertificateVerification)
	assert.Equal(t, true, config.DnsDiscover)

	config, err = esdb.ParseConnectionString("esdb://user:pass@localhost:2113?tls=true")
	require.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, "localhost:2113", config.Address)
	assert.Equal(t, false, config.DisableTLS)
	assert.Equal(t, false, config.DnsDiscover)
	assert.Equal(t, false, config.SkipCertificateVerification)
}

func endpointParse(addr string) *esdb.EndPoint {
	value, _ := esdb.ParseEndPoint(addr)
	return value
}

func TestConnectionStringWithValidClusterConnectionString(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://host1,host2,host3")
	assert.NoError(t, err)
	assert.Empty(t, config.Address)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:2113"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("host3:2113"), config.GossipSeeds[2])

	config, err = esdb.ParseConnectionString("esdb://host1:1234,host2:4321,host3:3231")
	assert.NoError(t, err)
	assert.Empty(t, config.Address)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:1234"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:4321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("host3:3231"), config.GossipSeeds[2])

	config, err = esdb.ParseConnectionString("esdb://user:pass@host1:1234,host2:4321,host3:3231?nodePreference=follower")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Empty(t, config.Address)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:1234"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:4321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("host3:3231"), config.GossipSeeds[2])
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)

	config, err = esdb.ParseConnectionString("esdb://host1,host2,host3?tls=false")
	assert.NoError(t, err)
	assert.Empty(t, config.Address)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:2113"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("host3:2113"), config.GossipSeeds[2])

	config, err = esdb.ParseConnectionString("esdb://host1,host2,host3?tlsVerifyCert=false")
	assert.NoError(t, err)
	assert.Empty(t, config.Address)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:2113"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("host3:2113"), config.GossipSeeds[2])
	assert.Equal(t, true, config.SkipCertificateVerification)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1,127.0.0.2:3321,127.0.0.3/?maxDiscoverAttempts=13&DiscoveryInterval=37&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=false")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Empty(t, config.Address)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("127.0.0.1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("127.0.0.2:3321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("127.0.0.3:2113"), config.GossipSeeds[2])

	config, err = esdb.ParseConnectionString("esdb://user:pass@host1,host2:3321,127.0.0.3/?tls=false&maxDiscoverAttempts=13&DiscoveryInterval=37&nOdEPrEfErence=FoLLoWer&tlsVerifyCert=false")
	assert.NoError(t, err)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Empty(t, config.Address)
	assert.Equal(t, 13, config.MaxDiscoverAttempts)
	assert.Equal(t, 37, config.DiscoveryInterval)
	assert.Equal(t, esdb.NodePreferenceFollower, config.NodePreference)
	require.NotEmpty(t, config.GossipSeeds)
	assert.Len(t, config.GossipSeeds, 3)
	assert.Equal(t, endpointParse("host1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("host2:3321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("127.0.0.3:2113"), config.GossipSeeds[2])
}

func TestConnectionStringWithDifferentTLSSettings(t *testing.T) {
	config, _ := esdb.ParseConnectionString("esdb://127.0.0.1/")
	assert.Equal(t, "127.0.0.1:2113", config.Address)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1?tls=true")
	assert.Equal(t, "127.0.0.1:2113", config.Address)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1/?tls=FaLsE")
	assert.Equal(t, "127.0.0.1:2113", config.Address)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3/")
	assert.Equal(t, endpointParse("127.0.0.1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("127.0.0.2:3321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("127.0.0.3:2113"), config.GossipSeeds[2])

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3?tls=true")
	assert.Equal(t, endpointParse("127.0.0.1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("127.0.0.2:3321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("127.0.0.3:2113"), config.GossipSeeds[2])

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3/?tls=fAlSe")
	assert.Equal(t, endpointParse("127.0.0.1:2113"), config.GossipSeeds[0])
	assert.Equal(t, endpointParse("127.0.0.2:3321"), config.GossipSeeds[1])
	assert.Equal(t, endpointParse("127.0.0.3:2113"), config.GossipSeeds[2])
}

func TestConnectionStringWithDifferentTLSVerifySettings(t *testing.T) {
	config, _ := esdb.ParseConnectionString("esdb://127.0.0.1/")
	assert.Equal(t, false, config.SkipCertificateVerification)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1/?tlsVerifyCert=TrUe")
	assert.Equal(t, false, config.SkipCertificateVerification)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1/?tlsVerifyCert=FaLsE")
	assert.Equal(t, true, config.SkipCertificateVerification)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3/")
	assert.Equal(t, false, config.SkipCertificateVerification)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3/?tlsVerifyCert=true")
	assert.Equal(t, false, config.SkipCertificateVerification)

	config, _ = esdb.ParseConnectionString("esdb://127.0.0.1,127.0.0.2:3321,127.0.0.3/?tlsVerifyCert=false")
	assert.Equal(t, true, config.SkipCertificateVerification)

}

func TestConnectionStringWithoutCertificateFile(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1")
	assert.Nil(t, err)
	assert.NotNil(t, config)
}

func TestConnectionStringWithCertificateFile(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?tlsCAFile=invalidPath")
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "open invalidPath")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?tlsCAFile=../certs/node/node.crt")
	assert.Nil(t, err)
	require.NotNil(t, config)
	assert.NotNil(t, config.RootCAs)
}

func TestConnectionStringWithKeepAlive(t *testing.T) {
	// KeepAliveInterval
	config, err := esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=zero")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid keepAliveInterval \"zero\". Please provide a positive integer, or -1 to disable")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=-2")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid keepAliveInterval \"-2\". Please provide a positive integer, or -1 to disable")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=-1")
	require.NoError(t, err)
	assert.Equal(t, -1, int(config.KeepAliveInterval))

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?KeepAliveInterval=100")
	require.NoError(t, err)
	assert.Equal(t, 100*time.Millisecond, config.KeepAliveInterval)

	// KeepAliveTimeout
	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveTimeout=one")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid keepAliveTimeout \"one\". Please provide a positive integer, or -1 to disable")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveTimeout=-3")
	require.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "Invalid keepAliveTimeout \"-3\". Please provide a positive integer, or -1 to disable")

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveTimeout=-1")
	require.NoError(t, err)
	assert.Equal(t, -1, int(config.KeepAliveTimeout))

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?KeepAliveTimeout=50000")
	require.NoError(t, err)
	assert.Equal(t, 50*time.Second, config.KeepAliveTimeout)

	// KeepAliveInterval & KeepAliveTimeout
	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=12000&KeepAliveTimeout=15000")
	require.NoError(t, err)
	assert.Equal(t, 12*time.Second, config.KeepAliveInterval)
	assert.Equal(t, 15*time.Second, config.KeepAliveTimeout)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=-1&KeepAliveTimeout=-1")
	require.NoError(t, err)
	assert.Equal(t, -1, int(config.KeepAliveInterval))
	assert.Equal(t, -1, int(config.KeepAliveTimeout))

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=-1&KeepAliveTimeout=15000")
	require.NoError(t, err)
	assert.Equal(t, -1, int(config.KeepAliveInterval))
	assert.Equal(t, 15*time.Second, config.KeepAliveTimeout)

	config, err = esdb.ParseConnectionString("esdb://user:pass@127.0.0.1/?keepAliveInterval=11000&KeepAliveTimeout=-1")
	require.NoError(t, err)
	assert.Equal(t, 11*time.Second, config.KeepAliveInterval)
	assert.Equal(t, -1, int(config.KeepAliveTimeout))
}

func TestConnectionStringWithDefaultDeadline(t *testing.T) {
	config, err := esdb.ParseConnectionString("esdb://localhost?defaultDeadline=60000")
	require.NoError(t, err)
	assert.NotNil(t, config.DefaultDeadline)
	assert.Equal(t, *config.DefaultDeadline, 60*time.Second)
}
