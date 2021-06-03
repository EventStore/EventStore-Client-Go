package subscription

import (
	"fmt"
	"io"
	"log"
	"sync"

	protoutils "github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	api "github.com/EventStore/EventStore-Client-Go/protos/subscription"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type PersistentSubscription struct {
	readClient                 api.PersistentSubscriptions_ReadClient
	subscriptionId             string
	started                    bool
	eventAppeared              func(messages.RecordedEvent) error
	subscriptionConfirmed      func(subscriptionID string)
	subscriptionDropped        func(reason string)
	subscriptionHasBeenDropped bool
	lock                       sync.Mutex
}

func NewPersistentSubscription(readClient api.PersistentSubscriptions_ReadClient, subscriptionId string, eventAppeared func(messages.RecordedEvent) error, subscriptionConfirmed func(subscriptionID string), subscriptionDropped func(reason string)) *PersistentSubscription {
	return &PersistentSubscription{
		readClient:            readClient,
		subscriptionId:        subscriptionId,
		eventAppeared:         eventAppeared,
		subscriptionConfirmed: subscriptionConfirmed,
		subscriptionDropped:   subscriptionDropped,
	}
}

func (subscription *PersistentSubscription) Stop() error {
	subscription.started = false
	if subscription.subscriptionDropped != nil && !subscription.subscriptionHasBeenDropped {
		subscription.subscriptionDropped(fmt.Sprintf("User initiated"))
		subscription.subscriptionHasBeenDropped = true
	}
	return subscription.readClient.CloseSend()
}

func (subscription *PersistentSubscription) IsRunning() bool {
	return subscription.started && !subscription.subscriptionHasBeenDropped
}
func (subscription *PersistentSubscription) Start() error {
	subscription.started = true
	go func() error {
		for subscription.started {
			readResult, err := subscription.readClient.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if subscription.subscriptionDropped != nil && !subscription.subscriptionHasBeenDropped {
					subscription.subscriptionDropped(fmt.Sprintf("PersistentSubscription dropped by server: %s", err.Error()))
					subscription.subscriptionHasBeenDropped = true
				}
				return fmt.Errorf("Failed to perform read. Reason: %v", err)
			}
			switch readResult.Content.(type) {
			case *api.ReadResp_SubscriptionConfirmation_:
				{
					confirmation := readResult.GetSubscriptionConfirmation()

					if subscription.subscriptionConfirmed != nil {
						subscription.subscriptionConfirmed(confirmation.SubscriptionId)
					}
				}
			case *api.ReadResp_Event:
				{
					event := readResult.GetEvent()
					recordedEvent := event.GetEvent()
					streamIdentifier := recordedEvent.GetStreamIdentifier()
					fmt.Printf("%+v\n", recordedEvent)
					if subscription.eventAppeared != nil {
						eventID := protoutils.EventIDFromSubscriptionProto(recordedEvent)
						err = subscription.eventAppeared(messages.RecordedEvent{
							EventID:        eventID,
							EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
							ContentType:    protoutils.GetContentTypeFromSubscriptionProto(recordedEvent),
							StreamID:       string(streamIdentifier.StreamName),
							EventNumber:    recordedEvent.GetStreamRevision(),
							CreatedDate:    protoutils.CreatedFromSubscriptionProto(recordedEvent),
							Position:       protoutils.PositionFromSubscriptionProto(recordedEvent),
							Data:           recordedEvent.GetData(),
							SystemMetadata: recordedEvent.GetMetadata(),
							UserMetadata:   recordedEvent.GetCustomMetadata(),
						})
						if err != nil {
							err = subscription.readClient.Send(protoutils.ToStreamPersistentSubscriptionNackRequest(subscription.subscriptionId, eventID, err))
							if err != nil {
								log.Println(err)
							} else {
								log.Printf("nack issued (subscription: %s) for event: %s\n", subscription.subscriptionId, eventID.String())
							}
						} else {
							err = subscription.readClient.Send(protoutils.ToStreamPersistentSubscriptionAckRequest(subscription.subscriptionId, eventID))
							if err != nil {
								log.Println(err)
							} else {
								log.Printf("ack issued (subscription: %s) for event: %s\n", subscription.subscriptionId, eventID.String())
							}
						}
					}
				}
			}
		}
		return nil
	}()
	return nil
}
