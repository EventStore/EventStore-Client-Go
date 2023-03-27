package esdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
)

func AuthenticationTests(t *testing.T, tlsDBClient, insecureDBClient *esdb.Client) {
	t.Run("AuthenticationTests", func(t *testing.T) {
		t.Run("callInsecureWithoutCredentials", callInsecureWithoutCredentials(insecureDBClient))
		t.Run("callInsecureWithInvalidCredentials", callInsecureWithInvalidCredentials(insecureDBClient))
		t.Run("callWithTLSAndDefaultCredentials", callWithTLSAndDefaultCredentials(tlsDBClient))
		t.Run("callWithTLSAndOverrideCredentials", callWithTLSAndOverrideCredentials(tlsDBClient))
		t.Run("callWithTLSAndInvalidOverrideCredentials", callWithTLSAndInvalidOverrideCredentials(tlsDBClient))
	})
}

func callInsecureWithoutCredentials(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), esdb.PersistentStreamSubscriptionOptions{})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}

func callInsecureWithInvalidCredentials(db *esdb.Client) TestCall {
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

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
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
