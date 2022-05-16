package esdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	api "github.com/EventStore/EventStore-Client-Go/v2/protos/streams"
)

// Subscription is a subscription's handle.
type Subscription struct {
	client *Client
	id     string
	inner  api.Streams_ReadClient
	cancel context.CancelFunc
	once   *sync.Once
	closed *int32
}

func newSubscription(client *Client, cancel context.CancelFunc, inner api.Streams_ReadClient, id string) *Subscription {
	once := new(sync.Once)
	closed := new(int32)

	atomic.StoreInt32(closed, 0)

	return &Subscription{
		client: client,
		id:     id,
		inner:  inner,
		once:   once,
		closed: closed,
		cancel: cancel,
	}
}

// Id returns subscription's id.
func (sub *Subscription) Id() string {
	return sub.id
}

// Close drops the subscription and cleans up allocated resources.
func (sub *Subscription) Close() error {
	sub.once.Do(func() {
		atomic.StoreInt32(sub.closed, 1)
		sub.cancel()
	})

	return nil
}

// Recv awaits for the next incoming subscription's event.
func (sub *Subscription) Recv() *SubscriptionEvent {
	if atomic.LoadInt32(sub.closed) != 0 {
		return &SubscriptionEvent{
			SubscriptionDropped: &SubscriptionDropped{
				Error: fmt.Errorf("subscription has been dropped"),
			},
		}
	}

	result, err := sub.inner.Recv()
	if err != nil {
		sub.client.grpcClient.logger.error("subscription has dropped. Reason: %v", err)

		dropped := SubscriptionDropped{
			Error: err,
		}

		atomic.StoreInt32(sub.closed, 1)
		return &SubscriptionEvent{
			SubscriptionDropped: &dropped,
		}
	}

	switch result.Content.(type) {
	case *api.ReadResp_Checkpoint_:
		{
			checkpoint := result.GetCheckpoint()
			position := Position{
				Commit:  checkpoint.CommitPosition,
				Prepare: checkpoint.PreparePosition,
			}

			return &SubscriptionEvent{
				CheckPointReached: &position,
			}
		}
	case *api.ReadResp_Event:
		{
			resolvedEvent := getResolvedEventFromProto(result.GetEvent())
			return &SubscriptionEvent{
				EventAppeared: &resolvedEvent,
			}
		}
	}

	panic("unreachable code")
}
