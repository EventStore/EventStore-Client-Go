package esdb

import (
	"context"
	"fmt"
	"log"
	"sync"

	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
)

type request struct {
	channel chan *SubscriptionEvent
}

type Subscription struct {
	client  *Client
	id      string
	inner   api.Streams_ReadClient
	channel chan request
	cancel  context.CancelFunc
	once    *sync.Once
}

func NewSubscription(client *Client, cancel context.CancelFunc, inner api.Streams_ReadClient, id string) *Subscription {
	channel := make(chan request)
	once := new(sync.Once)

	// It is not safe to consume a stream in different goroutines. This is why we only consume
	// the stream in a dedicated goroutine.
	//
	// Current implementation doesn't terminate the goroutine. When a subscription is dropped,
	// we keep user requests coming but will always send back a subscription dropped event.
	// This implementation is simple to maintain while letting the user sharing their subscription
	// among as many goroutines as they want.
	go func() {
		closed := false

		for {
			req := <-channel

			if closed {
				req.channel <- &SubscriptionEvent{
					SubscriptionDropped: &SubscriptionDropped{
						Error: fmt.Errorf("subscription has been dropped"),
					},
				}

				continue
			}

			result, err := inner.Recv()
			if err != nil {
				log.Printf("[error] subscription has dropped. Reason: %v", err)

				dropped := SubscriptionDropped{
					Error: err,
				}

				req.channel <- &SubscriptionEvent{
					SubscriptionDropped: &dropped,
				}

				closed = true

				continue
			}

			switch result.Content.(type) {
			case *api.ReadResp_Checkpoint_:
				{
					checkpoint := result.GetCheckpoint()
					position := Position{
						Commit:  checkpoint.CommitPosition,
						Prepare: checkpoint.PreparePosition,
					}

					req.channel <- &SubscriptionEvent{
						CheckPointReached: &position,
					}
				}
			case *api.ReadResp_Event:
				{
					resolvedEvent := getResolvedEventFromProto(result.GetEvent())
					req.channel <- &SubscriptionEvent{
						EventAppeared: &resolvedEvent,
					}
				}
			}
		}
	}()

	return &Subscription{
		client:  client,
		id:      id,
		inner:   inner,
		channel: channel,
		once:    once,
		cancel:  cancel,
	}
}

func (sub *Subscription) Id() string {
	return sub.id
}

func (sub *Subscription) Close() error {
	sub.once.Do(sub.cancel)
	return nil
}

func (sub *Subscription) Recv() *SubscriptionEvent {
	channel := make(chan *SubscriptionEvent)
	req := request{
		channel: channel,
	}

	sub.channel <- req
	resp := <-channel

	return resp
}
