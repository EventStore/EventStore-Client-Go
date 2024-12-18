package samples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func ReadFromStream(db *kurrent.Client) {
	// region read-from-stream
	options := kurrent.ReadStreamOptions{
		From:      kurrent.Start{},
		Direction: kurrent.Forwards,
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

func ReadFromStreamPosition(db *kurrent.Client) {
	// region read-from-stream-position
	ropts := kurrent.ReadStreamOptions{
		From: kurrent.Revision(10),
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

func ReadStreamOverridingUserCredentials(db *kurrent.Client) {
	// region overriding-user-credentials
	options := kurrent.ReadStreamOptions{
		From: kurrent.Start{},
		Authenticated: &kurrent.Credentials{
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

func ReadFromStreamPositionCheck(db *kurrent.Client) {
	// region checking-for-stream-presence
	ropts := kurrent.ReadStreamOptions{
		From: kurrent.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if err, ok := kurrent.FromError(err); !ok {
			if err.Code() == kurrent.ErrorCodeResourceNotFound {
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

func ReadStreamBackwards(db *kurrent.Client) {
	// region reading-backwards
	ropts := kurrent.ReadStreamOptions{
		Direction: kurrent.Backwards,
		From:      kurrent.End{},
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

func ReadFromAllStream(db *kurrent.Client) {
	// region read-from-all-stream
	options := kurrent.ReadAllOptions{
		From:      kurrent.Start{},
		Direction: kurrent.Forwards,
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

func IgnoreSystemEvents(db *kurrent.Client) {
	// region ignore-system-events
	stream, err := db.ReadAll(context.Background(), kurrent.ReadAllOptions{}, 100)

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

func ReadFromAllBackwards(db *kurrent.Client) {
	// region read-from-all-stream-backwards
	ropts := kurrent.ReadAllOptions{
		Direction: kurrent.Backwards,
		From:      kurrent.End{},
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

func ReadFromStreamResolvingLinkToS(db *kurrent.Client) {
	// region read-from-all-stream-resolving-link-Tos
	ropts := kurrent.ReadAllOptions{
		ResolveLinkTos: true,
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-from-all-stream-resolving-link-Tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

func ReadAllOverridingUserCredentials(db *kurrent.Client) {
	// region read-all-overriding-user-credentials
	ropts := kurrent.ReadAllOptions{
		From: kurrent.Start{},
		Authenticated: &kurrent.Credentials{
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
