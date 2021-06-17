package persistent

import (
	"errors"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
	"github.com/gofrs/uuid"
)

const MAX_ACK_COUNT = 2000

type connectionImpl struct {
	client                     protoClient
	subscriptionId             string
	started                    bool
	eventAppeared              EventAppearedHandler
	subscriptionDropped        SubscriptionDroppedHandler
	subscriptionHasBeenDropped bool
}

func (connection connectionImpl) Read() (*messages.RecordedEvent, error) {
	readResult, err := connection.client.Recv()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		if !connection.subscriptionHasBeenDropped {
			connection.subscriptionHasBeenDropped = true
		}

		if connection.subscriptionDropped != nil {
			connection.subscriptionDropped(fmt.Sprintf("Subscription dropped by server: %s", err.Error()))
		}
		panic(fmt.Errorf("failed to read from peristent connection. Subscription Id: %v Reason: %v",
			connection.subscriptionId, err))
	}
	switch readResult.Content.(type) {
	case *persistent.ReadResp_Event:
		{
			event := readResult.GetEvent()
			recordedEvent := event.GetEvent()

			if connection.eventAppeared != nil {
				err = connection.eventAppeared(newMessageFromProto(recordedEvent))

				if err == nil {
					// send Ack signal
				}

				// acknowledge message has been processed
				err = connection.client.Send(&persistent.ReadReq{
					Content: &persistent.ReadReq_Ack_{
						Ack: &persistent.ReadReq_Ack{
							Id:  []byte(connection.subscriptionId),
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

	return nil, nil
}

var Exceeds_Max_Message_Count_Err = errors.New(fmt.Sprintf("max messageID count exceeds limit of %v", MAX_ACK_COUNT))

func (connection connectionImpl) Ack(messageIds ...uuid.UUID) error {
	if len(messageIds) == 0 {
		return nil
	}

	if len(messageIds) > MAX_ACK_COUNT {
		return Exceeds_Max_Message_Count_Err
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(connection.subscriptionId),
				Ids: messageIdSliceToProto(messageIds...),
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (connection connectionImpl) Nack(reason string, action Nack_Action, messageIds ...uuid.UUID) error {
	if len(messageIds) == 0 {
		return nil
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(connection.subscriptionId),
				Ids:    messageIdSliceToProto(messageIds...),
				Action: persistent.ReadReq_Nack_Action(action),
				Reason: reason,
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func messageIdSliceToProto(messageIds ...uuid.UUID) []*shared.UUID {
	result := make([]*shared.UUID, len(messageIds))

	for index, messageId := range messageIds {
		result[index] = toProtoUUID(messageId)
	}

	return result
}

func newSubscriptionConnection(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	eventAppeared EventAppearedHandler,
	subscriptionDropped SubscriptionDroppedHandler,
) Connection {
	return &connectionImpl{
		client:                     client,
		subscriptionId:             subscriptionId,
		subscriptionHasBeenDropped: false,
		eventAppeared:              eventAppeared,
		subscriptionDropped:        subscriptionDropped,
	}
}

func newMessageFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) messages.RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return messages.RecordedEvent{
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
	}
}
