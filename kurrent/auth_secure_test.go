package kurrent_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func SecureAuthenticationTests(t *testing.T, client *kurrent.Client) {
	t.Run("AuthenticationTests", func(t *testing.T) {
		t.Run("callWithTLSAndDefaultCredentials", callWithTLSAndDefaultCredentials(client))
		t.Run("callWithTLSAndOverrideCredentials", callWithTLSAndOverrideCredentials(client))
		t.Run("callWithTLSAndInvalidOverrideCredentials", callWithTLSAndInvalidOverrideCredentials(client))
	})
}

func callWithTLSAndDefaultCredentials(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), kurrent.PersistentStreamSubscriptionOptions{})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}

func callWithTLSAndOverrideCredentials(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrent.PersistentStreamSubscriptionOptions{
			Authenticated: &kurrent.Credentials{
				Login:    "admin",
				Password: "changeit",
			},
		}
		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}

func callWithTLSAndInvalidOverrideCredentials(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrent.PersistentStreamSubscriptionOptions{
			Authenticated: &kurrent.Credentials{
				Login:    "invalid",
				Password: "invalid",
			},
		}
		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), opts)

		if err == nil {
			t.Fatalf("Unexpected succesfull completion!")
		}
	}
}
