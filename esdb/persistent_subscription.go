package esdb

import (
	"context"
	"fmt"
	"sync/atomic"

	"log"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/v2/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/v2/protos/shared"
	"github.com/gofrs/uuid"
)

type Nack_Action int32

const (
	Nack_Unknown Nack_Action = 0
	Nack_Park    Nack_Action = 1
	Nack_Retry   Nack_Action = 2
	Nack_Skip    Nack_Action = 3
	Nack_Stop    Nack_Action = 4
)

type PersistentSubscription struct {
	client         persistent.PersistentSubscriptions_ReadClient
	subscriptionId string
	once           *sync.Once
	closed         *int32
	cancel         context.CancelFunc
}

func (connection *PersistentSubscription) Recv() *PersistentSubscriptionEvent {
	if atomic.LoadInt32(connection.closed) != 0 {
		return &PersistentSubscriptionEvent{
			SubscriptionDropped: &SubscriptionDropped{
				Error: fmt.Errorf("subscription has been dropped"),
			},
		}
	}

	result, err := connection.client.Recv()
	if err != nil {
		atomic.StoreInt32(connection.closed, 1)

		log.Printf("[error] subscription has dropped. Reason: %v", err)

		dropped := SubscriptionDropped{
			Error: err,
		}

		return &PersistentSubscriptionEvent{
			SubscriptionDropped: &dropped,
		}
	}

	switch result.Content.(type) {
	case *persistent.ReadResp_Event:
		{
			resolvedEvent, retryCount := fromPersistentProtoResponse(result)
			return &PersistentSubscriptionEvent{
				EventAppeared: &EventAppeared{
					Event:      resolvedEvent,
					RetryCount: retryCount,
				},
			}
		}
	}

	panic("unreachable code")
}

func (connection *PersistentSubscription) Close() error {
	connection.once.Do(func() {
		atomic.StoreInt32(connection.closed, 1)
		connection.cancel()
		connection.client.CloseSend()
	})
	return nil
}

func (connection *PersistentSubscription) Ack(messages ...*ResolvedEvent) error {
	if len(messages) == 0 {
		return nil
	}

	var ids []uuid.UUID
	for _, event := range messages {
		ids = append(ids, event.OriginalEvent().EventID)
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(connection.subscriptionId),
				Ids: messageIdSliceToProto(ids...),
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (connection *PersistentSubscription) Nack(reason string, action Nack_Action, messages ...*ResolvedEvent) error {
	if len(messages) == 0 {
		return nil
	}

	ids := []uuid.UUID{}
	for _, event := range messages {
		ids = append(ids, event.OriginalEvent().EventID)
	}

	err := connection.client.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(connection.subscriptionId),
				Ids:    messageIdSliceToProto(ids...),
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

func NewPersistentSubscription(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	cancel context.CancelFunc,
) *PersistentSubscription {
	once := new(sync.Once)
	closed := new(int32)
	atomic.StoreInt32(closed, 0)

	return &PersistentSubscription{
		client:         client,
		subscriptionId: subscriptionId,
		once:           once,
		closed:         closed,
		cancel:         cancel,
	}
}
