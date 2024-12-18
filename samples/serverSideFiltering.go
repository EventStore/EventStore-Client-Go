package samples

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func ExcludeSystemEvents(db *kurrent.Client) {
	// region exclude-system
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: kurrent.ExcludeSystemEventsFilter(),
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

func EventTypePrefix(db *kurrent.Client) {
	// region event-type-prefix
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:     kurrent.EventFilterType,
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

func EventTypeRegex(db *kurrent.Client) {
	// region event-type-regex
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:  kurrent.EventFilterType,
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

func StreamPrefix(db *kurrent.Client) {
	// region stream-prefix
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:     kurrent.StreamFilterType,
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

func StreamRegex(db *kurrent.Client) {
	// region stream-regex
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:  kurrent.StreamFilterType,
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

func CheckpointCallbackWithInterval(db *kurrent.Client) {
	// region checkpoint-with-interval
	sub, err := db.SubscribeToAll(context.Background(), kurrent.SubscribeToAllOptions{
		Filter: &kurrent.SubscriptionFilter{
			Type:  kurrent.EventFilterType,
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
