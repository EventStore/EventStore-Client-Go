package subscription

import (
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type Subscription struct {
	readClient                 api.Streams_ReadClient
	subscriptionId             string
	started                    bool
	eventAppeared              chan<- messages.RecordedEvent
	checkpointReached          chan<- position.Position
	subscriptionDropped        chan<- string
	subscriptionHasBeenDropped bool
}

func NewSubscription(readClient api.Streams_ReadClient, subscriptionId string, eventAppeared chan <- messages.RecordedEvent, checkpointReached chan<- position.Position, subscriptionDropped chan<- string) *Subscription {
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
		subscription.subscriptionDropped <- "User initiated"
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
					subscription.subscriptionDropped <- fmt.Sprintf("Subscription dropped by server: %s", err.Error())
					subscription.subscriptionHasBeenDropped = true
				}
				return fmt.Errorf("Failed to perform read. Reason: %v", err)
			}
			switch readResult.Content.(type) {
			case *api.ReadResp_Checkpoint_:
				{
					if subscription.checkpointReached != nil {
						checkpoint := readResult.GetCheckpoint()

						subscription.checkpointReached <- position.Position{
							Commit:  checkpoint.CommitPosition,
							Prepare: checkpoint.PreparePosition,
						}
					}
				}
			case *api.ReadResp_Event:
				{
					if subscription.eventAppeared != nil {
						event := readResult.GetEvent()
						recordedEvent := event.GetEvent()
						streamIdentifier := recordedEvent.GetStreamIdentifier()

						subscription.eventAppeared <- messages.RecordedEvent{
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
						}
					}
				}
			}
		}
		return nil
	}()
	return nil
}
