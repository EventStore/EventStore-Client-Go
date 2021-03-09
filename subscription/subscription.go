package subscription

import (
	"fmt"
	"io"
	"time"

	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type Subscription struct {
	readClient                 api.Streams_ReadClient
	subscriptionId             string
	quit                       chan chan error
	eventAppeared              chan<- interface{}
	subscriptionDropped        chan<- string
}

func NewSubscription(readClient api.Streams_ReadClient, subscriptionId string, eventAppeared chan<- interface {},
					subscriptionDropped chan<- string) *Subscription {
	return &Subscription{
		readClient:          readClient,
		subscriptionId:      subscriptionId,
		eventAppeared:       eventAppeared,
		subscriptionDropped: subscriptionDropped,
		quit:                make(chan chan error),
	}
}

func (subscription *Subscription) Stop() error {
	errc := make(chan error)
	subscription.quit <- errc
	return <-errc
}

func (subscription *Subscription) Start() {
	go func() {
		type recvResult struct {
			result *api.ReadResp
			err    error
		}

		var err error
		var recvDone chan recvResult
		var subscriptionHasBeenDropped bool
		startRecv := time.After(0)
		for {
			select {
			case <-startRecv:
				recvDone = make(chan recvResult, 1)
				go func() {
					result, err := subscription.readClient.Recv()
					recvDone <- recvResult{result: result, err: err}
				}()
			case recvResult := <-recvDone:
				recvDone = nil
				result := recvResult.result
				err = recvResult.err
				startRecv = time.After(0)

				if err == io.EOF {
					break
				}
				if err != nil {
					if subscription.subscriptionDropped != nil && !subscriptionHasBeenDropped {
						subscription.subscriptionDropped <- fmt.Sprintf("Subscription dropped by server: %s", err.Error())
						subscriptionHasBeenDropped = true
					}
					err = fmt.Errorf("Failed to perform read. Reason: %v", err)
				}
				switch result.Content.(type) {
				case *api.ReadResp_Checkpoint_:
					if subscription.eventAppeared != nil {
						checkpoint := result.GetCheckpoint()

						subscription.eventAppeared <- messages.CheckpointEvent{
							CommitPosition:  checkpoint.CommitPosition,
							PreparePosition: checkpoint.PreparePosition,
						}
					}
				case *api.ReadResp_Event:
					if subscription.eventAppeared != nil {
						event := result.GetEvent()
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
			case errc := <-subscription.quit:
				if subscription.subscriptionDropped != nil && !subscriptionHasBeenDropped {
					subscription.subscriptionDropped <- "User initiated"
					subscriptionHasBeenDropped = true
				}

				if err != nil {
					errc <- err
					subscription.readClient.CloseSend()
				} else {
					errc <- subscription.readClient.CloseSend()
				}
				return
			}
		}
	}()
}
