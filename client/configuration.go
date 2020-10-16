package client

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	SchemeName = "esdb"
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

	return NewDefaultConfiguration(), nil
}
