package subscription

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"

	protoutils "github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	api "github.com/EventStore/EventStore-Client-Go/protos/subscription"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type PersistentSubscriptionHandler interface {
	EventAppeared(context.Context, messages.RecordedEvent) error
	SubscriptionConfirmed(subscriptionID string)
	SubscriptionDropped(reason string)
	NotifyAck(ctx context.Context, subscriptionID string, eventID string, err error)
	NotifyNack(ctx context.Context, subscriptionID string, eventID string, err error)
}

type PersistentSubscription struct {
	client            api.PersistentSubscriptionsClient
	readClient        api.PersistentSubscriptions_ReadClient
	subscriptionId    string
	groupName         string
	streamID          string
	bufferSize        int32
	started           bool
	handler           PersistentSubscriptionHandler
	reconnectionDelay int

	lock sync.Mutex
}

func (subscription *PersistentSubscription) OnConnectionUpdate(conn *grpc.ClientConn) {
	subscription.lock.Lock()
	defer subscription.lock.Unlock()
	subscription.client = api.NewPersistentSubscriptionsClient(conn)
	if subscription.readClient != nil {
		err := subscription.readClient.CloseSend()
		if err != nil {
			log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).WithError(err).Warn("Failed to close read client")
		}
		subscription.readClient = nil
		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).Debug("Closed previous readClient")
		}
	}
	if log.IsLevelEnabled(log.DebugLevel) {
		log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).Debug("OnConnectionUpdate completed")
	}
}

func NewPersistentSubscription(groupName string, streamID string, bufferSize int32, reconnectionDelay int, handler PersistentSubscriptionHandler) Subscription {
	ps := &PersistentSubscription{
		groupName:         groupName,
		streamID:          streamID,
		bufferSize:        bufferSize,
		handler:           handler,
		reconnectionDelay: reconnectionDelay,
	}
	return ps
}

func (subscription *PersistentSubscription) Stop() error {
	subscription.lock.Lock()
	defer subscription.lock.Unlock()
	subscription.started = false
	subscription.handler.SubscriptionDropped("User initiated")
	return subscription.readClient.CloseSend()
}

func (subscription *PersistentSubscription) reconnect(ctx context.Context) error {
	subscription.lock.Lock()
	defer subscription.lock.Unlock()
	log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).Info("Connecting subscription stream")
	readClient, err := subscription.client.Read(ctx)
	if err != nil {
		return err
	}
	err = readClient.Send(protoutils.ToStreamPersistentSubscriptionRequest(subscription.groupName, subscription.streamID, subscription.bufferSize))
	if err != nil {
		return err
	}
	readResult, err := readClient.Recv()
	if err != nil {
		return err
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_SubscriptionConfirmation_:
		{
			confirmation := readResult.GetSubscriptionConfirmation()
			// Close previous if exists
			if subscription.readClient != nil {
				err := subscription.readClient.CloseSend()
				if err != nil {
					log.Warnf("failed to close previous client. Reason: %v", err)
				}
			}

			subscription.readClient = readClient
			subscription.subscriptionId = confirmation.SubscriptionId
			subscription.handler.SubscriptionConfirmed(subscription.subscriptionId)
			log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).Info("Subscription connected")
			return nil
		}
	default:
		readClient.CloseSend()
		return fmt.Errorf("unexpected server response upon subscription")
	}
}

func (subscription *PersistentSubscription) receive() error {
	subscription.lock.Lock()
	defer subscription.lock.Unlock()
	ctx := context.Background()

	readResult, err := subscription.readClient.Recv()
	if err == io.EOF {
		subscription.readClient = nil
		return err
	}
	if err != nil {
		subscription.handler.SubscriptionDropped(fmt.Sprintf("PersistentSubscription dropped by server: %s", err.Error()))
		subscription.readClient = nil
		return err
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Event:
		{
			event := readResult.GetEvent()
			recordedEvent := event.GetEvent()
			streamIdentifier := recordedEvent.GetStreamIdentifier()

			ackID := protoutils.EventIDFromSubscriptionProto(event.GetEvent())
			if event.GetLink() != nil {
				ackID = protoutils.EventIDFromSubscriptionProto(event.GetLink())
			}

			eventID := protoutils.EventIDFromSubscriptionProto(recordedEvent)
			err = subscription.handler.EventAppeared(ctx, messages.RecordedEvent{
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
				err = subscription.readClient.Send(protoutils.ToStreamPersistentSubscriptionNackRequest(subscription.subscriptionId, ackID, err))
				if err == io.EOF {
					subscription.readClient = nil
					return err
				}
				subscription.handler.NotifyNack(ctx, subscription.subscriptionId, ackID.String(), err)
			} else {
				err = subscription.readClient.Send(protoutils.ToStreamPersistentSubscriptionAckRequest(subscription.subscriptionId, ackID))
				if err == io.EOF {
					subscription.readClient = nil
					return err
				}
				subscription.handler.NotifyAck(ctx, subscription.subscriptionId, ackID.String(), err)
			}
			return nil
		}
	default:
		return fmt.Errorf("unexpected server response upon receive")
	}
}

func (subscription *PersistentSubscription) Start() error {
	subscription.started = true
	log.WithField("subscription", fmt.Sprintf("%s::%s", subscription.groupName, subscription.streamID)).Info("Starting subscription")
	go func() {
		for subscription.started {
			if subscription.readClient == nil {
				err := subscription.reconnect(context.Background())
				if err != nil {
					log.WithError(err).Warn("Failed to reconnect")
					time.Sleep(time.Duration(subscription.reconnectionDelay) * time.Millisecond)
				}
			} else {
				err := subscription.receive()
				if err != nil {
					log.WithError(err).Warn("Failed to receive message")
				}
			}
		}
		return
	}()
	return nil
}
