package samples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
)

func ReadFromStream(db *esdb.Client) {
	// region read-from-stream
	options := esdb.ReadStreamOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}
	stream, err := db.ReadStream(context.Background(), "some-stream", options, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadFromStreamPosition(db *esdb.Client) {
	// region read-from-stream-position
	ropts := esdb.ReadStreamOptions{
		From: esdb.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 20)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream-position
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadStreamOverridingUserCredentials(db *esdb.Client) {
	// region overriding-user-credentials
	options := esdb.ReadStreamOptions{
		From: esdb.Start{},
		Authenticated: &esdb.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	}
	stream, err := db.ReadStream(context.Background(), "some-stream", options, 100)
	// endregion overriding-user-credentials

	if err != nil {
		panic(err)
	}

	stream.Close()
}

func ReadFromStreamPositionCheck(db *esdb.Client) {
	// region checking-for-stream-presence
	ropts := esdb.ReadStreamOptions{
		From: esdb.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if err, ok := esdb.FromError(err); !ok {
			if err.Code() == esdb.ErrorCodeResourceNotFound {
				fmt.Print("Stream not found")
			} else if errors.Is(err, io.EOF) {
				break
			} else {
				panic(err)
			}
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion checking-for-stream-presence
}

func ReadStreamBackwards(db *esdb.Client) {
	// region reading-backwards
	ropts := esdb.ReadStreamOptions{
		Direction: esdb.Backwards,
		From:      esdb.End{},
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 10)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion reading-backwards
}

func ReadFromAllStream(db *esdb.Client) {
	// region read-from-all-stream
	options := esdb.ReadAllOptions{
		From:      esdb.Start{},
		Direction: esdb.Forwards,
	}
	stream, err := db.ReadAll(context.Background(), options, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream
	// region read-from-all-stream-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-iterate
}

func IgnoreSystemEvents(db *esdb.Client) {
	// region ignore-system-events
	stream, err := db.ReadAll(context.Background(), esdb.ReadAllOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)

		if strings.HasPrefix(event.OriginalEvent().EventType, "$") {
			continue
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion ignore-system-events
}

func ReadFromAllBackwards(db *esdb.Client) {
	// region read-from-all-stream-backwards
	ropts := esdb.ReadAllOptions{
		Direction: esdb.Backwards,
		From:      esdb.End{},
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream-backwards
	// region read-from-all-stream-backwards-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-backwards-iterate
}

func ReadFromStreamResolvingLinkToS(db *esdb.Client) {
	// region read-from-all-stream-resolving-link-Tos
	ropts := esdb.ReadAllOptions{
		ResolveLinkTos: true,
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-from-all-stream-resolving-link-Tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

func ReadAllOverridingUserCredentials(db *esdb.Client) {
	// region read-all-overriding-user-credentials
	ropts := esdb.ReadAllOptions{
		From: esdb.Start{},
		Authenticated: &esdb.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	}
	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-all-overriding-user-credentials

	if err != nil {
		panic(err)
	}

	stream.Close()
}
