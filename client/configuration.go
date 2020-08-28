package client

// Configuration ...
type Configuration struct {
	Address                     string
	GossipSeeds                 []string
	UseTls                      bool
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
