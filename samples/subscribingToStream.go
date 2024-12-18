package samples

import (
	"context"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func SubscribeToStream(db *kurrent.Client) {
	options := kurrent.SubscribeToStreamOptions{}
	// region subscribe-to-stream
	stream, err := db.SubscribeToStream(context.Background(), "some-stream", kurrent.SubscribeToStreamOptions{})

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// endregion subscribe-to-stream

	// region subscribe-to-stream-from-position
	db.SubscribeToStream(context.Background(), "some-stream", kurrent.SubscribeToStreamOptions{
		From: kurrent.Revision(20),
	})
	// endregion subscribe-to-stream-from-position

	// region subscribe-to-stream-live
	options = kurrent.SubscribeToStreamOptions{
		From: kurrent.End{},
	}

	db.SubscribeToStream(context.Background(), "some-stream", options)
	// endregion subscribe-to-stream-live

	// region subscribe-to-stream-resolving-linktos
	options = kurrent.SubscribeToStreamOptions{
		From:           kurrent.Start{},
		ResolveLinkTos: true,
	}

	db.SubscribeToStream(context.Background(), "$et-myEventType", options)
	// endregion subscribe-to-stream-resolving-linktos

	// region subscribe-to-stream-subscription-dropped
	options = kurrent.SubscribeToStreamOptions{
		From: kurrent.Start{},
	}

	for {

		stream, err := db.SubscribeToStream(context.Background(), "some-stream", options)

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for {
			event := stream.Recv()

			if event.SubscriptionDropped != nil {
				stream.Close()
				break
			}

			if event.EventAppeared != nil {
				// handles the event...
				options.From = kurrent.Revision(event.EventAppeared.OriginalEvent().EventNumber)
			}
		}
	}
	// endregion subscribe-to-stream-subscription-dropped
}

func SubscribeToAll(db *kurrent.Client) {
	options := kurrent.SubscribeToAllOptions{}
	// region subscribe-to-all
	stream, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{})

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event := stream.Recv()

		if event.EventAppeared != nil {
			// handles the event...
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// endregion subscribe-to-all

	// region subscribe-to-all-from-position
	db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		From: kurrent.Position{
			Commit:  1_056,
			Prepare: 1_056,
		},
	})
	// endregion subscribe-to-all-from-position

	// region subscribe-to-all-live
	db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		From: kurrent.End{},
	})
	// endregion subscribe-to-all-live

	// region subscribe-to-all-subscription-dropped
	options = kurrent.SubscribeToAllOptions{
		From: kurrent.Start{},
	}

	for {
		stream, err := db.SubscribeToAll(context.Background(), options)

		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for {
			event := stream.Recv()

			if event.SubscriptionDropped != nil {
				stream.Close()
				break
			}

			if event.EventAppeared != nil {
				// handles the event...
				options.From = event.EventAppeared.OriginalEvent().Position
			}
		}
	}
	// endregion subscribe-to-all-subscription-dropped
}

func SubscribeToFiltered(db *kurrent.Client) {
	// region stream-prefix-filtered-subscription
	db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:     kurrent.StreamFilterType,
			Prefixes: []string{"test-"},
		},
	})
	// endregion stream-prefix-filtered-subscription
	// region stream-regex-filtered-subscription
	db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:  kurrent.StreamFilterType,
			Regex: "/invoice-\\d\\d\\d/g",
		},
	})
	// endregion stream-regex-filtered-subscription
}

func SubscribeToAllOverridingUserCredentials(db *kurrent.Client) {
	// region overriding-user-credentials
	db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Authenticated: &kurrent.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	})
	// endregion overriding-user-credentials
}
