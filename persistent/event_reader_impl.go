package persistent

import (
	"context"
	"io"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

const MAX_ACK_COUNT = 2000

type eventReaderImpl struct {
	protoClient        persistent.PersistentSubscriptions_ReadClient
	subscriptionId     string
	messageAdapter     messageAdapter
	readRequestChannel chan chan readResponse
	cancel             context.CancelFunc
	once               sync.Once
}

func (reader *eventReaderImpl) ReadOne() (ReadResponseEvent, errors.Error) {
	channel := make(chan readResponse)

	reader.readRequestChannel <- channel
	resp := <-channel

	return resp.ReadResponseEvent, resp.Error
}

func (reader *eventReaderImpl) readOne() (ReadResponseEvent, errors.Error) {
	protoResponse, protoErr := reader.protoClient.Recv()
	if protoErr != nil {
		if protoErr == io.EOF {
			return ReadResponseEvent{}, errors.NewError(errors.EndOfStream, protoErr)
		}
		trailer := reader.protoClient.Trailer()
		err := connection.GetErrorFromProtoException(trailer, protoErr)
		if err != nil {
			return ReadResponseEvent{}, err
		}
		return ReadResponseEvent{}, errors.NewError(errors.FatalError, protoErr)
	}

	result := reader.messageAdapter.fromProtoResponse(protoResponse.GetEvent())
	return result, nil
}

func (reader *eventReaderImpl) Close() error {
	reader.once.Do(reader.cancel)
	return nil
}

const Exceeds_Max_Message_Count_Err errors.ErrorCode = "Exceeds_Max_Message_Count_Err"

func (reader *eventReaderImpl) Ack(messages ...ReadResponseEvent) errors.Error {
	if len(messages) == 0 {
		return nil
	}

	if len(messages) > MAX_ACK_COUNT {
		return errors.NewErrorCode(Exceeds_Max_Message_Count_Err)
	}

	ids := []uuid.UUID{}
	for _, event := range messages {
		ids = append(ids, event.GetOriginalEvent().EventID)
	}

	protoErr := reader.protoClient.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(reader.subscriptionId),
				Ids: messageIdSliceToProto(ids...),
			},
		},
	})

	trailers := reader.protoClient.Trailer()
	if protoErr != nil {
		return connection.GetErrorFromProtoException(trailers, protoErr)
	}

	return nil
}

func (reader *eventReaderImpl) Nack(reason string,
	action Nack_Action, messages ...ReadResponseEvent) error {
	if len(messages) == 0 {
		return nil
	}

	ids := []uuid.UUID{}
	for _, event := range messages {
		ids = append(ids, event.GetOriginalEvent().EventID)
	}

	err := reader.protoClient.Send(&persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(reader.subscriptionId),
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

func (reader *eventReaderImpl) readLoopWithRequest() {
	for {
		responseChannel := <-reader.readRequestChannel
		result, err := reader.readOne()

		responseChannel <- readResponse{
			ReadResponseEvent: result,
			Error:             err,
		}
	}
}

func messageIdSliceToProto(messageIds ...uuid.UUID) []*shared.UUID {
	result := make([]*shared.UUID, len(messageIds))

	for index, messageId := range messageIds {
		result[index] = toProtoUUID(messageId)
	}

	return result
}

type readResponse struct {
	ReadResponseEvent ReadResponseEvent
	Error             errors.Error
}

func newEventReader(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	messageAdapter messageAdapter,
	cancel context.CancelFunc,
) EventReader {
	channel := make(chan chan readResponse)

	reader := &eventReaderImpl{
		protoClient:        client,
		subscriptionId:     subscriptionId,
		messageAdapter:     messageAdapter,
		readRequestChannel: channel,
		cancel:             cancel,
	}

	// It is not safe to consume a stream in different goroutines. This is why we only consume
	// the stream in a dedicated goroutine.
	//
	// Current implementation doesn't terminate the goroutine. When a subscription is dropped,
	// we keep user requests coming but will always send back a subscription dropped event.
	// This implementation is simple to maintain while letting the user sharing their subscription
	// among as many goroutines as they want.
	go reader.readLoopWithRequest()

	return reader
}
