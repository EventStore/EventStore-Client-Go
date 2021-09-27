package connection

import (
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	SchemeDefaultPort       = 2113
	SchemaHostsSeparator    = ","
	SchemeName              = "esdb"
	SchemeNameWithDiscover  = "esdb+discover"
	SchemePathSeparator     = "/"
	SchemePortSeparator     = ":"
	SchemeQuerySeparator    = "?"
	SchemeSeparator         = "://"
	SchemeSettingSeparator  = "&"
	SchemeUserInfoSeparator = "@"
)

// Configuration describes how to connect to an instance of EventStoreDB.
type Configuration struct {
	// The URI of the EventStoreDB.
	// Use this when connecting to a single node or when DNS Discovery is set to true.
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
	DiscoveryInterval uint64 // Defaults to 100 milliseconds.

	// The amount of time (in seconds) after which an attempt to discover gossip will fail.
	GossipTimeout int // Defaults to 5 seconds.

	// Specifies if DNS discovery should be used.
	// If set to true use Address field as DNS address.
	DnsDiscover bool // Defaults to false.

	// The amount of time (in milliseconds) to wait after which a keepalive ping is sent on the transport.
	// If set below 10s, a minimum value of 10s will be used instead. Use -1 to disable. Use -1 to disable.
	KeepAliveInterval time.Duration // Defaults to 10 seconds.

	// The amount of time (in milliseconds) the sender of the keep alive ping waits for an acknowledgement.
	KeepAliveTimeout time.Duration // Defaults to 10 seconds.
}

func (conf Configuration) getCandidates() []string {
	var candidates []string
	if conf.DnsDiscover {
		candidates = append(candidates, conf.Address)
	} else {
		for _, seed := range conf.GossipSeeds {
			candidates = append(candidates, seed.String())
		}
	}

	return candidates
}

// ParseConnectionString creates a Configuration based on an EventStoreDb connection string.
func ParseConnectionString(connectionString string) (*Configuration, error) {
	config := &Configuration{
		DiscoveryInterval:   100,
		GossipTimeout:       5,
		MaxDiscoverAttempts: 10,
		KeepAliveInterval:   10 * time.Second,
		KeepAliveTimeout:    10 * time.Second,
	}

	schemeIndex := strings.Index(connectionString, SchemeSeparator)
	if schemeIndex == -1 {
		return nil, fmt.Errorf("The scheme is missing from the connection string")
	}

	scheme := connectionString[:schemeIndex]
	if scheme != SchemeName && scheme != SchemeNameWithDiscover {
		return nil, fmt.Errorf("An invalid scheme is specified, expecting esdb:// or esdb+discover://")
	}
	currentConnectionString := connectionString[schemeIndex+len(SchemeSeparator):]

	config.DnsDiscover = scheme == SchemeNameWithDiscover

	userInfoIndex, err := parseUserInfo(currentConnectionString, config)
	if err != nil {
		return nil, err
	}
	if userInfoIndex != -1 {
		currentConnectionString = currentConnectionString[userInfoIndex:]
	}

	var host, path, settings string
	settingsIndex := strings.Index(currentConnectionString, SchemeQuerySeparator)
	hostIndex := strings.IndexAny(currentConnectionString, SchemePathSeparator+SchemeQuerySeparator)
	if hostIndex == -1 {
		host = currentConnectionString
		currentConnectionString = ""
	} else {
		host = currentConnectionString[:hostIndex]
		path = currentConnectionString[hostIndex:]
	}

	if settingsIndex != -1 {
		path = currentConnectionString[hostIndex:settingsIndex]
		settings = strings.TrimLeft(currentConnectionString[settingsIndex:], SchemeQuerySeparator)
	}

	path = strings.TrimLeft(path, SchemePathSeparator)
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
	userInfoIndex := strings.Index(s, SchemeUserInfoSeparator)
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

		return userInfoIndex + len(SchemeUserInfoSeparator), nil
	}

	return -1, nil
}

func parseSettings(settings string, config *Configuration) error {
	if settings == "" {
		return nil
	}

	kvPairs := make(map[string]string)
	settingTokens := strings.Split(settings, SchemeSettingSeparator)

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
		err := parseUintSetting(k, v, &config.DiscoveryInterval)
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
			log.Printf("Specified KeepAliveInterval of %d is less than recommended 10_000 ms", config.KeepAliveInterval)
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

func parseUintSetting(k, v string, i *uint64) error {
	var err error
	*i, err = strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fmt.Errorf("Setting '%s' must be an integer value", k)
	}

	return nil
}

func parseNodePreference(v string, config *Configuration) error {
	switch strings.ToLower(v) {
	case "follower":
		config.NodePreference = NodePreference_Follower
	case "leader":
		config.NodePreference = NodePreference_Leader
	case "random":
		config.NodePreference = NodePreference_Random
	case "readonlyreplica":
		config.NodePreference = NodePreference_ReadOnlyReplica
	default:
		return fmt.Errorf("Invalid NodePreference: '%s'", v)
	}

	return nil
}

func parseHost(host string, config *Configuration) error {
	endpoints := make([]*EndPoint, 0)
	hosts := strings.Split(host, SchemaHostsSeparator)
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

type NodePreference string

const (
	NodePreference_Leader          NodePreference = "Leader"
	NodePreference_Follower        NodePreference = "Follower"
	NodePreference_ReadOnlyReplica NodePreference = "ReadOnlyReplica"
	NodePreference_Random          NodePreference = "Random"
)

func (nodePreference NodePreference) String() string {
	return string(nodePreference)
}
