package esdb

// Credentials holds a login and a password for authenticated requests.
type Credentials struct {
	// User's login.
	Login string
	// User's password.
	Password string
}
