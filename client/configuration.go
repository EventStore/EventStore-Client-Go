package client

import (
	"fmt"
	"net/url"
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

	if strings.TrimLeft(u.Path, SchemePathSeperator) != "" {
		return nil, fmt.Errorf("The connection string cannot have a path")
	}

	return config, nil
}
