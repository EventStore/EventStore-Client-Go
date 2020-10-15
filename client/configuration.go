package client

import (
	"net/url"
	"strings"

	client_errors "github.com/EventStore/EventStore-Client-Go/errors"
)

const (
	SchemeName              = "esdb"
	SchemeSeparator         = "://"
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

func ParseConfig(connectionString string) (*Configuration, error) {
	u, err := url.Parse(connectionString)
	if err != nil {
		if strings.Contains(err.Error(), "missing protocol scheme") {
			return nil, client_errors.ErrNoSchemeSpecified
		}
		return nil, err
	}

	if u.Scheme  != SchemeName {
		return nil, client_errors.ErrInvalidSchemeSpecified
	}

	if u.User != nil {
		userName := u.User.Username()
		_, isPasswordSet := u.User.Password()
		if userName == "" || !isPasswordSet {
			return nil, client_errors.ErrInvalidUserCredentials
		}
	}

	return &Configuration{}, nil
}
