package persistent

import (
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type (
	EventAppearedHandler       func(messages.RecordedEvent) error
	SubscriptionDroppedHandler func(reason string)
)

type PersistentSubscriptionConnection struct {
	client                     protoClient
	subscriptionId             string
	started                    bool
	eventAppeared              EventAppearedHandler
	subscriptionDropped        SubscriptionDroppedHandler
	subscriptionHasBeenDropped bool
}

func NewPersistentSubscriptionConnection(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	eventAppeared EventAppearedHandler,
	subscriptionDropped SubscriptionDroppedHandler,
) *PersistentSubscriptionConnection {
	return &PersistentSubscriptionConnection{
		client:                     client,
		subscriptionId:             subscriptionId,
		subscriptionHasBeenDropped: false,
		eventAppeared:              eventAppeared,
		subscriptionDropped:        subscriptionDropped,
	}
}

func (subscription *PersistentSubscriptionConnection) Start() {
}

func (subscription *PersistentSubscriptionConnection) Stop() {
	// subscription.client.Send()
}

func (subscription *PersistentSubscriptionConnection) Start2() {
	subscription.started = true
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				fmt.Println(err)
			}
			// send quit signal
		}()

		subscription.readMessages()
	}()
}

func (subscription *PersistentSubscriptionConnection) readMessages() {
SubscriptionRead:
	for subscription.started {
		readResult, err := subscription.client.Recv()
		if err == io.EOF {
			break SubscriptionRead
		}
		if err != nil {
			if !subscription.subscriptionHasBeenDropped {
				subscription.subscriptionHasBeenDropped = true
			}

			if subscription.subscriptionDropped != nil {
				subscription.subscriptionDropped(fmt.Sprintf("Subscription dropped by server: %s", err.Error()))
			}
			panic(fmt.Errorf("Failed to perform read. Reason: %v", err))
		}
		switch readResult.Content.(type) {
		case *persistent.ReadResp_Event:
			{
				event := readResult.GetEvent()
				recordedEvent := event.GetEvent()
				streamIdentifier := recordedEvent.GetStreamIdentifier()

				if subscription.eventAppeared != nil {
					subscription.eventAppeared(messages.RecordedEvent{
						EventID:        eventIDFromProto(recordedEvent),
						EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
						ContentType:    getContentTypeFromProto(recordedEvent),
						StreamID:       string(streamIdentifier.StreamName),
						EventNumber:    recordedEvent.GetStreamRevision(),
						CreatedDate:    createdFromProto(recordedEvent),
						Position:       positionFromProto(recordedEvent),
						Data:           recordedEvent.GetData(),
						SystemMetadata: recordedEvent.GetMetadata(),
						UserMetadata:   recordedEvent.GetCustomMetadata(),
					})

					err = subscription.client.Send(&persistent.ReadReq{
						Content: &persistent.ReadReq_Ack_{
							Ack: &persistent.ReadReq_Ack{
								Id:  []byte(subscription.subscriptionId),
								Ids: []*shared.UUID{recordedEvent.GetId()},
							},
						},
					})
					// handle error
					if err != nil {
						fmt.Print(err)
					}
				}
			}
		}
	}
}
