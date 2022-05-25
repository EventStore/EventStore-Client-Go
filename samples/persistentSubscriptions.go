package samples

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
)

func createPersistentSubscription(client *esdb.Client) {
	// #region create-persistent-subscription-to-stream
	err := client.CreatePersistentSubscription(context.Background(), "test-stream", "subscription-group", esdb.PersistentStreamSubscriptionOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion create-persistent-subscription-to-stream
}

func connectToPersistentSubscriptionToStream(client *esdb.Client) {
	// #region subscribe-to-persistent-subscription-to-stream
	sub, err := client.SubscribeToPersistentSubscription(context.Background(), "test-stream", "subscription-group", esdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-to-stream
}

func connectToPersistentSubscriptionToAll(client *esdb.Client) {
	// #region subscribe-to-persistent-subscription-to-all
	sub, err := client.SubscribeToPersistentSubscriptionToAll(context.Background(), "subscription-group", esdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-to-all
}

func createPersistentSubscriptionToAll(client *esdb.Client) {
	// #region create-persistent-subscription-to-all
	options := esdb.PersistentAllSubscriptionOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.StreamFilterType,
			Prefixes: []string{"test"},
		},
	}

	err := client.CreatePersistentSubscriptionToAll(context.Background(), "subscription-group", options)

	if err != nil {
		panic(err)
	}
	// #endregion create-persistent-subscription-to-all
}

func connectToPersistentSubscriptionWithManualAcks(client *esdb.Client) {
	// #region subscribe-to-persistent-subscription-with-manual-acks
	sub, err := client.SubscribeToPersistentSubscription(context.Background(), "test-stream", "subscription-group", esdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-with-manual-acks
}

func updatePersistentSubscription(client *esdb.Client) {
	// #region update-persistent-subscription
	options := esdb.PersistentStreamSubscriptionOptions{
		Settings: &esdb.PersistentSubscriptionSettings{
			ResolveLinkTos:       true,
			CheckpointLowerBound: 20,
		},
	}

	err := client.UpdatePersistentSubscription(context.Background(), "test-stream", "subscription-group", options)

	if err != nil {
		panic(err)
	}
	// #endregion update-persistent-subscription
}

func deletePersistentSubscription(client *esdb.Client) {
	// #region delete-persistent-subscription
	err := client.DeletePersistentSubscription(context.Background(), "test-stream", "subscription-group", esdb.DeletePersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion delete-persistent-subscription
}
