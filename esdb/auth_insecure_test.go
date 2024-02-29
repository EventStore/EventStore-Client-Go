package esdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
)

func InsecureAuthenticationTests(t *testing.T, client *esdb.Client) {
	t.Run("AuthenticationTests", func(t *testing.T) {
		t.Run("callInsecureWithoutCredentials", callInsecureWithoutCredentials(client))
		t.Run("callInsecureWithInvalidCredentials", callInsecureWithInvalidCredentials(client))
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
