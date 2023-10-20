package esdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
)

func SecureAuthenticationTests(t *testing.T, client *esdb.Client) {
	t.Run("AuthenticationTests", func(t *testing.T) {
		t.Run("callWithTLSAndDefaultCredentials", callWithTLSAndDefaultCredentials(client))
		t.Run("callWithTLSAndOverrideCredentials", callWithTLSAndOverrideCredentials(client))
		t.Run("callWithTLSAndInvalidOverrideCredentials", callWithTLSAndInvalidOverrideCredentials(client))
	})
}

func callWithTLSAndDefaultCredentials(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), esdb.PersistentStreamSubscriptionOptions{})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}

func callWithTLSAndOverrideCredentials(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := esdb.PersistentStreamSubscriptionOptions{
			Authenticated: &esdb.Credentials{
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

func callWithTLSAndInvalidOverrideCredentials(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := esdb.PersistentStreamSubscriptionOptions{
			Authenticated: &esdb.Credentials{
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
