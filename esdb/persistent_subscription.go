package esdb

import (
	"context"
	"fmt"
	"sync/atomic"

	"sync"

	"github.com/EventStore/EventStore-Client-Go/v2/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/v2/protos/shared"
	"github.com/gofrs/uuid"
)

// NackAction persistent subscription acknowledgement error type.
type NackAction int32

const (
	// NackActionUnknown client does not know what action to take, let the server decide.
	NackActionUnknown NackAction = 0
	// NackActionPark park message, do not resend.
	NackActionPark NackAction = 1
	// NackActionRetry explicitly retry the message.
	NackActionRetry NackAction = 2
	// NackActionSkip skip this message, do not resend, do not park the message.
	NackActionSkip NackAction = 3
	// NackActionStop stop the subscription.
	NackActionStop NackAction = 4
)

// PersistentSubscription persistent subscription handle.
type PersistentSubscription struct {
	client         persistent.PersistentSubscriptions_ReadClient
	subscriptionId string
	once           *sync.Once
	closed         *int32
	cancel         context.CancelFunc
	logger         *logger
}

// Recv awaits for the next incoming persistent subscription event.
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

		connection.logger.error("subscription has dropped. Reason: %v", err)

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

// Close drops the persistent subscription and free allocated resources.
func (connection *PersistentSubscription) Close() error {
	connection.once.Do(func() {
		atomic.StoreInt32(connection.closed, 1)
		connection.cancel()
		connection.client.CloseSend()
	})
	return nil
}

// Ack acknowledges events have been successfully processed.
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

// Nack acknowledges events failed processing.
func (connection *PersistentSubscription) Nack(reason string, action NackAction, messages ...*ResolvedEvent) error {
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

func newPersistentSubscription(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	cancel context.CancelFunc,
	logger *logger,
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
		logger:         logger,
	}
}
