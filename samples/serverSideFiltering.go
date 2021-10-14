package samples

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/esdb"
)

func ExcludeSystemEvents(db *esdb.Client) {
	// region exclude-system
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: esdb.ExcludeSystemEventsFilter(),
	})

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion exclude-system
}

func EventTypePrefix(db *esdb.Client) {
	// region event-type-prefix
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.EventFilterType,
			Prefixes: []string{"customer-"},
		},
	})

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion event-type-prefix
}

func EventTypeRegex(db *esdb.Client) {
	// region event-type-regex
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:  esdb.EventFilterType,
			Regex: "^user|^company",
		},
	})

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion event-type-regex
}

func StreamPrefix(db *esdb.Client) {
	// region stream-prefix
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.StreamFilterType,
			Prefixes: []string{"user-"},
		},
	})

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion stream-prefix
}

func StreamRegex(db *esdb.Client) {
	// region stream-regex
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:  esdb.StreamFilterType,
			Regex: "^user|^company",
		},
	})

	if err != nil {
		panic(err)
	}

	defer sub.Close()

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion stream-regex
}

func CheckpointCallbackWithInterval(db *esdb.Client) {
	// region checkpoint-with-interval
	sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		Filter: &esdb.SubscriptionFilter{
			Type:  esdb.EventFilterType,
			Regex: "/^[^\\$].*/",
		},
	})
	// endregion checkpoint-with-interval
	if err != nil {
		panic(err)
	}

	defer sub.Close()

	// region checkpoint
	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			streamId := event.EventAppeared.OriginalEvent().StreamID
			revision := event.EventAppeared.OriginalEvent().EventNumber

			fmt.Printf("received event %v@%v", revision, streamId)
		}

		if event.CheckPointReached != nil {
			fmt.Printf("checkpoint taken at %v", event.CheckPointReached.Prepare)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}

	// endregion checkpoint
}
