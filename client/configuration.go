package client

import (
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
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

// Configuration ...
type Configuration struct {
	Address                     string
	GossipSeeds                 []string
	DisableTLS                  bool
	NodePreference              NodePreference
	Username                    string
	Password                    string
	SkipCertificateVerification bool
	Connected                   bool
	MaxDiscoverAttempts         int
	DiscoveryInterval           int
	GossipTimeout               int
	DnsDiscover                 bool
	RootCAs                     *x509.CertPool
}

func ParseConnectionString(connectionString string) (*Configuration, error) {
	config := &Configuration{
		DiscoveryInterval:   100,
		GossipTimeout:       5,
		MaxDiscoverAttempts: 10,
	}

	schemeIndex := strings.Index(connectionString, SchemeSeparator)
	if schemeIndex == -1 {
		return nil, fmt.Errorf("The scheme is missing from the connection string")
	}

	scheme := connectionString[:schemeIndex]
	if scheme != SchemeName && scheme != SchemeNameWithDiscover {
		return nil, fmt.Errorf("An invalid scheme is specified, expecting esdb:// or esdb+discover://")
	}
	currentConnectionString := connectionString[schemeIndex + len(SchemeSeparator):]

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

func parseKeyValuePair(s string) (string, string, error){
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

	return  nil
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
	parsedHosts := make([]string, 0)
	hosts := strings.Split(host, SchemaHostsSeparator)
	for _, host := range hosts {
		if host == "" {
			return fmt.Errorf("An empty host is specified")
		}

		hostName := host
		port := SchemeDefaultPort
		if strings.Contains(host, SchemePortSeparator) {
			tokens := strings.Split(host, SchemePortSeparator)
			if len(tokens) != 2 {
				return fmt.Errorf("Too many colons specified in host, expecting {host}:{port}")
			}

			var err error
			port, err = strconv.Atoi(tokens[1])
			if err != nil {
				return fmt.Errorf("Invalid port specified, expecting an integer value")
			}

			hostName = tokens[0]
		}

		parsedHosts = append(parsedHosts, fmt.Sprintf("%s:%d", hostName, port))
	}

	if len(parsedHosts) == 1 {
		config.Address = parsedHosts[0]
	} else {
		config.GossipSeeds = parsedHosts
	}

	return nil
}