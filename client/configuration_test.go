package client_test

import (
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	client_errors "github.com/EventStore/EventStore-Client-Go/errors"
)

func TestConnectionStringWithNoSchema(t *testing.T) {
	_, err := client.ParseConfig(":so/mething/random")

	if !errors.Is(err, client_errors.ErrNoSchemeSpecified) {
		t.Fatalf("Expected ErrNoSchemeSpecified, got %+v", err)
	}
}

func TestConnectionStringWithInvalidScheme(t *testing.T) {
	_, err := client.ParseConfig("esdbwrong://")
	if !errors.Is(err, client_errors.ErrInvalidSchemeSpecified) {
		t.Fatalf("Expected ErrInvalidSchemeSpecified, got %+v", err)
	}

	_, err = client.ParseConfig("wrong://")
	if !errors.Is(err, client_errors.ErrInvalidSchemeSpecified) {
		t.Fatalf("Expected ErrInvalidSchemeSpecified, got %+v", err)
	}

	_, err = client.ParseConfig("badesdb://")
	if !errors.Is(err, client_errors.ErrInvalidSchemeSpecified) {
		t.Fatalf("Expected ErrInvalidSchemeSpecified, got %+v", err)
	}
}
