package esdb

import (
	"crypto/x509"
	"fmt"
	url2 "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	schemeName             = "esdb"
	schemeNameWithDiscover = "esdb+discover"
)

var basepath string

func init() {
	cwd, _ := os.Getwd()
	basepath = cwd
}

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

	// The path to the file containing the X.509 user certificate in PEM format.
	UserCertFile string

	// The path to the file containing the user certificate’s matching private key in PEM format
	UserKeyFile string

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

func (conf *Configuration) Validate() error {
	if (conf.UserCertFile == "") != (conf.UserKeyFile == "") {
		return fmt.Errorf("both userCertFile and userKeyFile must be provided")
	}

	return nil
}

func initConfiguration() *Configuration {
	return &Configuration{
		DiscoveryInterval:   100,
		GossipTimeout:       5,
		MaxDiscoverAttempts: 10,
		KeepAliveInterval:   10 * time.Second,
		KeepAliveTimeout:    10 * time.Second,
		NodePreference:      NodePreferenceLeader,
		Logger:              ConsoleLogging(),
	}
}

// ParseConnectionString creates a Configuration based on an EventStoreDb connection string.
func ParseConnectionString(connectionString string) (*Configuration, error) {
	url, err := url2.Parse(connectionString)

	if err != nil {
		// The way we support gossip seeds in cluster configuration is not supported by
		// the URL standard. When it happens, we rewrite the connection string to parse
		// those seeds properly. It happens when facing connection string like the following:
		//
		// esdb://host1:1234,host2:4321,host3:3231
		if !strings.Contains(connectionString, ",") {
			return nil, err
		}

		desugarConnectionString := strings.ReplaceAll(connectionString, ",", "/")
		url, newErr := url2.Parse(desugarConnectionString)

		// In this case it should mean the connection is truly invalid, so we return
		// the previous error
		if newErr != nil {
			return nil, err
		}

		if len(url.Host) == 0 {
			return nil, err
		}

		config := initConfiguration()
		endpoints := make([]*EndPoint, 0)
		ept, err := parseEndpoint(url.Host)

		if err != nil {
			return nil, err
		}

		endpoints = append(endpoints, ept)

		for _, host := range strings.Split(url.Path, "/") {
			if len(host) == 0 {
				continue
			}

			ept, err = parseEndpoint(host)

			if err != nil {
				return nil, err
			}

			endpoints = append(endpoints, ept)
		}

		config.GossipSeeds = endpoints
		return parseFromUrl(config, url)
	}

	return parseFromUrl(initConfiguration(), url)
}

func parseEndpoint(host string) (*EndPoint, error) {
	var endpoint *EndPoint

	if len(host) == 0 {
		return nil, fmt.Errorf("empty host, if you use multiple gossip seeds, maybe one of them is empty")
	}

	splits := strings.Split(host, ":")

	switch len(splits) {
	case 1:
		endpoint = &EndPoint{
			Host: host,
			Port: 2_113,
		}
	case 2:
		if len(splits[1]) != 0 {
			if p, err := strconv.Atoi(splits[1]); err == nil {
				endpoint = &EndPoint{
					Host: splits[0],
					Port: uint16(p),
				}
			} else {
				return nil, fmt.Errorf("wrong port format '%s': %v", splits[1], err)
			}
		} else {
			return nil, fmt.Errorf("wrong port format: %s", host)
		}
	default:
		return nil, fmt.Errorf("wrong host format: %s", host)
	}

	return endpoint, nil
}

func parseFromUrl(config *Configuration, url *url2.URL) (*Configuration, error) {
	if url.Scheme != schemeName && url.Scheme != schemeNameWithDiscover {
		return nil, fmt.Errorf("an invalid scheme is specified, expecting esdb:// or esdb+discover://")
	}

	config.DnsDiscover = url.Scheme == schemeNameWithDiscover

	if url.User != nil {
		config.Username = url.User.Username()
		password, _ := url.User.Password()
		config.Password = password

		if len(config.Username) == 0 {
			return nil, fmt.Errorf("credentials were provided with an empty username")
		}
	}

	if len(config.GossipSeeds) == 0 && len(url.Path) > 0 && url.Path != "/" {
		return nil, fmt.Errorf("unsupported URL path: %v", url.Path)
	}

	if len(config.GossipSeeds) == 0 && len(url.Host) == 0 {
		return nil, fmt.Errorf("connection string doesn't have an host")
	}

	if len(config.GossipSeeds) == 0 {
		if !strings.Contains(url.Host, ",") {
			ept, err := parseEndpoint(url.Host)

			if err != nil {
				return nil, err
			}

			config.Address = fmt.Sprintf("%v:%d", ept.Host, ept.Port)
		} else {
			config.GossipSeeds = make([]*EndPoint, 0)

			for _, host := range strings.Split(url.Host, ",") {
				ept, err := parseEndpoint(host)

				if err != nil {
					return nil, err
				}

				config.GossipSeeds = append(config.GossipSeeds, ept)
			}
		}
	}

	for key, values := range url.Query() {
		if len(values) == 0 {
			continue
		}

		err := parseSetting(key, values[0], config)
		if err != nil {
			return nil, err
		}
	}

	return config, nil
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
	case "usercertfile":
		config.UserCertFile = resolvePath(v)
	case "userkeyfile":
		config.UserKeyFile = resolvePath(v)
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
		config.applyLogger(LogWarn, "Unknown setting: %s", k)
	}

	return nil
}

func parseCertificateFile(certFile string, config *Configuration) error {
	b, err := os.ReadFile(certFile)
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

func resolvePath(rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}

	return filepath.Join(basepath, rel)
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
