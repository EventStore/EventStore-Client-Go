package client

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	SchemaHostsSeperator = ","
	SchemeName         	 = "esdb"
	SchemePathSeperator  = "/"
	SchemaSeperator      = "://"
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
}

// NewConfiguration ...
func NewDefaultConfiguration() *Configuration {
	return &Configuration{
		Address:                     "localhost:2113",
		Username:                    "admin",
		Password:                    "changeit",
		SkipCertificateVerification: false,
	}
}

func ParseConnectionString(connectionString string) (*Configuration, error) {
	config := NewDefaultConfiguration()
	u, err := url.Parse(connectionString)
	if err != nil {
		if strings.Contains(err.Error(), "missing protocol scheme") {
			return nil, fmt.Errorf("The scheme is missing from the connection string, details: %s", err.Error())
		}
		return nil, fmt.Errorf("The connection string is invalid, details: %s", err.Error())
	}

	if u.Scheme != SchemeName {
		return nil, fmt.Errorf("An invalid scheme is specified, expecting esdb://")
	}

	if u.User != nil {
		username := u.User.Username()
		if username == "" {
			return nil, fmt.Errorf("The specified username is empty")
		}

		password, isPasswordSet := u.User.Password()
		if !isPasswordSet {
			return nil, fmt.Errorf("The password is not set")
		}

		if password == "" {
			return nil, fmt.Errorf("The specified password is empty")
		}
	}

	hosts := strings.Split(u.Host, SchemaHostsSeperator)
	for _, host := range hosts {
		if host == "" {
			return nil, fmt.Errorf("An empty host is specified")
		}

		schemaPrefix := fmt.Sprintf("%s%s", SchemeName, SchemaSeperator)
		parsableHost := fmt.Sprintf("%s%s", schemaPrefix, host)
		_, err := url.Parse(parsableHost)
		if err != nil {
			errorWithoutScheme := strings.Replace(err.Error(), schemaPrefix, "", 1)
			return nil, fmt.Errorf("The specified host is invalid, details %s", errorWithoutScheme)
		}
	}

	path := strings.TrimLeft(u.Path, SchemePathSeperator)
	if path != "" {
		return nil, fmt.Errorf("The specified path must be either an empty string or a forward slash (/) but the following path was found instead: '%s'", path)
	}

	err = parseSettings(u.Query())
	if err != nil {
		return nil, err
	}

	return config, nil
}

func parseSettings(urlValues url.Values) error {
	settings := make(map[string]string)
	for key, values := range urlValues {
		normalizedKey := strings.ToLower(key)
		duplicateKeyError := fmt.Errorf("The connection string cannot have duplicate key/value pairs, found: '%s'", key)
		if len(values) > 1 {
			return duplicateKeyError
		}

		if _, ok := settings[normalizedKey]; ok {
			return duplicateKeyError
		} else {
			if len(values) == 0 || values[0] == "" {
				return fmt.Errorf("No value specified for: '%s'", key)
			}
			settings[normalizedKey] = values[0]
			err := parseSetting(key, values[0])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func parseSetting(k, v string ) error {
	normalizedKey := strings.ToLower(k)
	switch normalizedKey {
	case "discoveryinterval":
		_, err := parseIntSetting(k, v)
		if err != nil {
			return err
		}
	case "gossiptimeout":
		_, err := parseIntSetting(k, v)
		if err != nil {
			return err
		}
	case "maxdiscoverattempts":
		_, err := parseIntSetting(k, v)
		if err != nil {
			return err
		}
	case "nodepreference":
		switch v {
		case "leader":
		case "follower":
		case "random":
		case "readonlyreplica":
		default:
			return fmt.Errorf("Invalid NodePreference: '%s'", v)
		}
	case "tlsverifycert":
		_, err := parseBoolSetting(k, v)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unknown setting: '%s'", k)
	}

	return nil
}

func parseBoolSetting(k, v string) (int, error) {
	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("Setting '%s' must be either true or false", k)
	}

	return i, nil
}

func parseIntSetting(k, v string) (int, error) {
	i, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("Setting '%s' must be an integer value", k)
	}

	return i, nil
}