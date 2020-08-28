package subscription

import (
	"fmt"
	"io"

	protoutils "github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type Subscription struct {
	readClient                 api.Streams_ReadClient
	subscriptionId             string
	started                    bool
	eventAppeared              func(messages.RecordedEvent)
	checkpointReached          func(position.Position)
	subscriptionDropped        func(reason string)
	subscriptionHasBeenDropped bool
}

func NewSubscription(readClient api.Streams_ReadClient, subscriptionId string, eventAppeared func(messages.RecordedEvent), checkpointReached func(position.Position), subscriptionDropped func(reason string)) *Subscription {
	return &Subscription{
		readClient:          readClient,
		subscriptionId:      subscriptionId,
		eventAppeared:       eventAppeared,
		checkpointReached:   checkpointReached,
		subscriptionDropped: subscriptionDropped,
	}
}

func (subscription *Subscription) Stop() error {
	subscription.started = false
	if subscription.subscriptionDropped != nil && !subscription.subscriptionHasBeenDropped {
		subscription.subscriptionDropped(fmt.Sprintf("User initiated"))
		subscription.subscriptionHasBeenDropped = true
	}
	return subscription.readClient.CloseSend()
}

func (subscription *Subscription) Start() error {
	subscription.started = true
	go func() error {
		for subscription.started {
			readResult, err := subscription.readClient.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if subscription.subscriptionDropped != nil && !subscription.subscriptionHasBeenDropped {
					subscription.subscriptionDropped(fmt.Sprintf("Subscription dropped by server: %s", err.Error()))
					subscription.subscriptionHasBeenDropped = true
				}
				return fmt.Errorf("Failed to perform read. Reason: %v", err)
			}
			switch readResult.Content.(type) {
			case *api.ReadResp_Checkpoint_:
				{
					checkpoint := readResult.GetCheckpoint()
					if subscription.checkpointReached != nil {
						subscription.checkpointReached(position.Position{
							Commit:  checkpoint.CommitPosition,
							Prepare: checkpoint.PreparePosition,
						})
					}
				}
			case *api.ReadResp_Event:
				{
					event := readResult.GetEvent()
					recordedEvent := event.GetEvent()
					streamIdentifier := recordedEvent.GetStreamIdentifier()

					if subscription.eventAppeared != nil {
						subscription.eventAppeared(messages.RecordedEvent{
							EventID:        protoutils.EventIDFromProto(recordedEvent),
							EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
							ContentType:    protoutils.GetContentTypeFromProto(recordedEvent),
							StreamID:       string(streamIdentifier.StreamName),
							EventNumber:    recordedEvent.GetStreamRevision(),
							CreatedDate:    protoutils.CreatedFromProto(recordedEvent),
							Position:       protoutils.PositionFromProto(recordedEvent),
							Data:           recordedEvent.GetData(),
							SystemMetadata: recordedEvent.GetMetadata(),
							UserMetadata:   recordedEvent.GetCustomMetadata(),
						})
					}
				}
			}
		}
		return nil
	}()
	return nil
}
