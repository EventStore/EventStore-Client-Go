package client

import (
	"strings"

	client_errors "github.com/EventStore/EventStore-Client-Go/errors"
)

const (
	SchemeName      = "esdb"
	SchemeSeparator = "://"
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
	schemeIndex := strings.Index(connectionString, SchemeSeparator)
	if schemeIndex == -1 {
		return nil, client_errors.ErrNoSchemeSpecified
	}

	scheme := connectionString[:schemeIndex]
	if scheme != SchemeName {
		return nil, client_errors.ErrInvalidSchemeSpecified
	}

	return &Configuration{}, nil
}
