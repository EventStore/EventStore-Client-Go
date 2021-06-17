package persistent

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/gofrs/uuid"
)

const MAX_ACK_COUNT = 2000

type syncConnectionImpl struct {
	client         protoClient
	subscriptionId string
	messageAdapter messageAdapter
}

func (connection *syncConnectionImpl) Read() (*messages.RecordedEvent, error) {
	readResult, err := connection.client.Recv()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf("failed to read from peristent connection. Subscription Id: %v Reason: %v",
				connection.subscriptionId, err))
	}

	switch readResult.Content.(type) {
	case *persistent.ReadResp_Event:
		{
			message := connection.messageAdapter.FromProtoResponse(readResult)
			return message, nil
		}
	case *persistent.ReadResp_SubscriptionConfirmation_:
		return nil, errors.New("received subscription confirmation while reading for events")
	}

	contentType := reflect.TypeOf(readResult.Content).Name()
	return nil, errors.New(fmt.Sprintf("unknown content type received %v", contentType))
}

var Exceeds_Max_Message_Count_Err = errors.New(fmt.Sprintf("max messageID count exceeds limit of %v", MAX_ACK_COUNT))

func (connection *syncConnectionImpl) Ack(messageIds ...uuid.UUID) error {
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

func (connection *syncConnectionImpl) Nack(reason string, action Nack_Action, messageIds ...uuid.UUID) error {
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
	messageAdapter messageAdapter,
) SyncReadConnection {
	return &syncConnectionImpl{
		client:         client,
		subscriptionId: subscriptionId,
		messageAdapter: messageAdapter,
	}
}