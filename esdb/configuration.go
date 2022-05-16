package esdb

import (
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

const (
	schemeDefaultPort       = 2113
	schemaHostsSeparator    = ","
	schemeName              = "esdb"
	schemeNameWithDiscover  = "esdb+discover"
	schemePathSeparator     = "/"
	schemePortSeparator     = ":"
	schemeQuerySeparator    = "?"
	schemeSeparator         = "://"
	schemeSettingSeparator  = "&"
	schemeUserInfoSeparator = "@"
)

// Configuration describes how to connect to an instance of EventStoreDB.
type Configuration struct {
	// The URI of the EventStoreDB. Use this when connecting to a single node.
	// Example: localhost:2113
	Address string

	// An array of end points used to seed gossip.
	GossipSeeds []*EndPoint

	// Disable communicating over a secure channel.
	DisableTLS bool // Defaults to false.

	// The NodePreference to use when connecting.
	NodePreference NodePreference

	// The username to use for authenticating against the EventStoreDB instance.
	Username string

	// The password to use for authenticating against the EventStoreDB instance.
	Password string

	// RootCAs defines the set of root certificate authorities
	// that clients use when verifying server certificates.
	// If RootCAs is nil, TLS uses the host's root CA set.
	RootCAs *x509.CertPool // Defaults to nil.

	// Allows to skip certificate validation.
	SkipCertificateVerification bool // Defaults to false.

	// The maximum number of times to attempt end point discovery.
	MaxDiscoverAttempts int // Defaults to 10.

	// The polling interval (in milliseconds) used to discover the end point.
	DiscoveryInterval int // Defaults to 100 milliseconds.

	// The amount of time (in seconds) after which an attempt to discover gossip will fail.
	GossipTimeout int // Defaults to 5 seconds.

	// Specifies if DNS discovery should be used.
	DnsDiscover bool // Defaults to false.

	// The amount of time (in milliseconds) to wait after which a keepalive ping is sent on the transport.
	// If set below 10s, a minimum value of 10s will be used instead. Use -1 to disable. Use -1 to disable.
	KeepAliveInterval time.Duration // Defaults to 10 seconds.

	// The amount of time (in milliseconds) the sender of the keep alive ping waits for an acknowledgement.
	KeepAliveTimeout time.Duration // Defaults to 10 seconds.

	// The amount of time (in milliseconds) a non-streaming operation should take to complete before resulting in a
	// DeadlineExceeded. Defaults to 10 seconds.
	DefaultDeadline *time.Duration

	// Logging abstraction used by the client.
	Logger LoggingFunc
}

func (conf *Configuration) applyLogger(level LogLevel, format string, args ...interface{}) {
	if conf.Logger != nil {
		conf.Logger(level, format, args)
	}
}

// ParseConnectionString creates a Configuration based on an EventStoreDb connection string.
func ParseConnectionString(connectionString string) (*Configuration, error) {
	config := &Configuration{
		DiscoveryInterval:   100,
		GossipTimeout:       5,
		MaxDiscoverAttempts: 10,
		KeepAliveInterval:   10 * time.Second,
		KeepAliveTimeout:    10 * time.Second,
		NodePreference:      NodePreferenceLeader,
		Logger:              ConsoleLogging(),
	}

	schemeIndex := strings.Index(connectionString, schemeSeparator)
	if schemeIndex == -1 {
		return nil, fmt.Errorf("The scheme is missing from the connection string")
	}

	scheme := connectionString[:schemeIndex]
	if scheme != schemeName && scheme != schemeNameWithDiscover {
		return nil, fmt.Errorf("An invalid scheme is specified, expecting esdb:// or esdb+discover://")
	}
	currentConnectionString := connectionString[schemeIndex+len(schemeSeparator):]

	config.DnsDiscover = scheme == schemeNameWithDiscover

	userInfoIndex, err := parseUserInfo(currentConnectionString, config)
	if err != nil {
		return nil, err
	}
	if userInfoIndex != -1 {
		currentConnectionString = currentConnectionString[userInfoIndex:]
	}

	var host, path, settings string
	settingsIndex := strings.Index(currentConnectionString, schemeQuerySeparator)
	hostIndex := strings.IndexAny(currentConnectionString, schemePathSeparator+schemeQuerySeparator)
	if hostIndex == -1 {
		host = currentConnectionString
		currentConnectionString = ""
	} else {
		host = currentConnectionString[:hostIndex]
		path = currentConnectionString[hostIndex:]
	}

	if settingsIndex != -1 {
		path = currentConnectionString[hostIndex:settingsIndex]
		settings = strings.TrimLeft(currentConnectionString[settingsIndex:], schemeQuerySeparator)
	}

	path = strings.TrimLeft(path, schemePathSeparator)
	if len(path) > 0 {
		return nil, fmt.Errorf("The specified path must be either an empty string or a forward slash (/) but the following path was found instead: '%s'", path)
	}

	err = parseSettings(settings, config)
	if err != nil {
		return nil, err
	}

	err = parseHost(host, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func parseUserInfo(s string, config *Configuration) (int, error) {
	userInfoIndex := strings.Index(s, schemeUserInfoSeparator)
	if userInfoIndex != -1 {
		userInfo := s[0:userInfoIndex]
		tokens := strings.Split(userInfo, ":")
		if len(tokens) != 2 {
			return -1, fmt.Errorf("The user credentials are invalid and are expected to be in format {username}:{password}")
		}

		username := tokens[0]
		if username == "" {
			return -1, fmt.Errorf("The specified username is empty")
		}

		password := tokens[1]
		if password == "" {
			return -1, fmt.Errorf("The specified password is empty")
		}

		config.Username = username
		config.Password = password

		return userInfoIndex + len(schemeUserInfoSeparator), nil
	}

	return -1, nil
}

func parseSettings(settings string, config *Configuration) error {
	if settings == "" {
		return nil
	}

	kvPairs := make(map[string]string)
	settingTokens := strings.Split(settings, schemeSettingSeparator)

	for _, settingToken := range settingTokens {
		key, value, err := parseKeyValuePair(settingToken)
		if err != nil {
			return err
		}

		normalizedKey := strings.ToLower(key)
		duplicateKeyError := fmt.Errorf("The connection string cannot have duplicate key/value pairs, found: '%s'", key)

		if _, ok := kvPairs[normalizedKey]; ok {
			return duplicateKeyError
		} else {
			if value == "" {
				return fmt.Errorf("No value specified for setting: '%s'", key)
			}
			kvPairs[normalizedKey] = value
			err := parseSetting(key, value, config)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func parseKeyValuePair(s string) (string, string, error) {
	keyValueTokens := strings.Split(s, "=")
	if len(keyValueTokens) != 2 {
		return "", "", fmt.Errorf("Invalid key/value pair specified in connection string, expecting {key}={value} got: '%s'", s)
	}

	return keyValueTokens[0], keyValueTokens[1], nil
}

func parseSetting(k, v string, config *Configuration) error {
	normalizedKey := strings.ToLower(k)
	switch normalizedKey {
	case "discoveryinterval":
		err := parseIntSetting(k, v, &config.DiscoveryInterval)
		if err != nil {
			return err
		}
	case "gossiptimeout":
		err := parseIntSetting(k, v, &config.GossipTimeout)
		if err != nil {
			return err
		}
	case "maxdiscoverattempts":
		err := parseIntSetting(k, v, &config.MaxDiscoverAttempts)
		if err != nil {
			return err
		}
	case "nodepreference":
		err := parseNodePreference(v, config)
		if err != nil {
			return err
		}
	case "keepaliveinterval":
		err := parseKeepAliveSetting(k, v, &config.KeepAliveInterval)
		if err != nil {
			return err
		}

		if config.KeepAliveInterval >= 0 && config.KeepAliveInterval < 10*time.Second {
			config.applyLogger(LogWarn, "specified KeepAliveInterval of %d is less than recommended 10_000 ms", config.KeepAliveInterval)
		}
	case "keepalivetimeout":
		err := parseKeepAliveSetting(k, v, &config.KeepAliveTimeout)
		if err != nil {
			return err
		}
	case "tls":
		err := parseBoolSetting(k, v, &config.DisableTLS, true)
		if err != nil {
			return err
		}
	case "tlscafile":
		err := parseCertificateFile(v, config)
		if err != nil {
			return err
		}
	case "tlsverifycert":
		err := parseBoolSetting(k, v, &config.SkipCertificateVerification, true)
		if err != nil {
			return err
		}
	case "defaultdeadline":
		config.DefaultDeadline = new(time.Duration)
		err := parseDurationAsMs(k, v, config.DefaultDeadline)
		if err != nil {
			return nil
		}
	default:
		return fmt.Errorf("Unknown setting: '%s'", k)
	}

	return nil
}

func parseCertificateFile(certFile string, config *Configuration) error {
	b, err := ioutil.ReadFile(certFile)
	if err != nil {
		return err
	}

	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return fmt.Errorf("failed to append certificate file")
	}

	config.RootCAs = cp

	return nil
}

func parseBoolSetting(k, v string, b *bool, inverse bool) error {
	var err error
	*b, err = strconv.ParseBool(strings.ToLower(v))
	if err != nil {
		return fmt.Errorf("Setting '%s' must be either true or false", k)
	}

	*b = *b != inverse

	return nil
}

func parseIntSetting(k, v string, i *int) error {
	var err error
	*i, err = strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("Setting '%s' must be an integer value", k)
	}

	return nil
}

func parseNodePreference(v string, config *Configuration) error {
	switch strings.ToLower(v) {
	case "follower":
		config.NodePreference = NodePreferenceFollower
	case "leader":
		config.NodePreference = NodePreferenceLeader
	case "random":
		config.NodePreference = NodePreferenceRandom
	case "readonlyreplica":
		config.NodePreference = NodePreferenceReadOnlyReplica
	default:
		return fmt.Errorf("Invalid NodePreference: '%s'", v)
	}

	return nil
}

func parseHost(host string, config *Configuration) error {
	endpoints := make([]*EndPoint, 0)
	hosts := strings.Split(host, schemaHostsSeparator)
	for _, host := range hosts {
		endpoint, err := ParseEndPoint(host)

		if err != nil {
			return err
		}

		endpoints = append(endpoints, endpoint)
	}

	if len(endpoints) == 1 {
		config.Address = endpoints[0].String()
	} else {
		config.GossipSeeds = endpoints
	}

	return nil
}

func parseKeepAliveSetting(k, v string, d *time.Duration) error {
	i, err := strconv.Atoi(v)
	if err != nil || i < -1 {
		return fmt.Errorf("Invalid %s \"%s\". Please provide a positive integer, or -1 to disable", k, v)
	}

	if i == -1 {
		*d = -1
	} else {
		*d = time.Duration(i * int(time.Millisecond))
	}

	return nil
}

func parseDurationAsMs(k, v string, d *time.Duration) error {
	i, err := strconv.Atoi(v)
	if err != nil || i < 1 {
		return fmt.Errorf("invalid %s \"%s\". Please provide a strictly positive integer", k, v)
	}

	*d = time.Duration(i * int(time.Millisecond))

	return nil
}

// NodePreference indicates which order of preferred nodes for connecting to.
type NodePreference string

const (
	// NodePreferenceLeader When attempting connection, prefers leader nodes.
	NodePreferenceLeader NodePreference = "Leader"
	// NodePreferenceFollower When attempting connection, prefers follower nodes.
	NodePreferenceFollower NodePreference = "Follower"
	// NodePreferenceReadOnlyReplica When attempting connection, prefers read only replica nodes.
	NodePreferenceReadOnlyReplica NodePreference = "ReadOnlyReplica"
	// NodePreferenceRandom When attempting connection, has no node preference.
	NodePreferenceRandom NodePreference = "Random"
)

func (nodePreference NodePreference) String() string {
	return string(nodePreference)
}
