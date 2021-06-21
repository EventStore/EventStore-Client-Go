package persistent

import (
	"errors"
	"io"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_SyncReadConnection_ReadEOF(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)
	protoClient.EXPECT().Recv().Return(nil, io.EOF)

	syncReadConnection := newSyncReadConnection(protoClient, "some id", nil)

	result, err := syncReadConnection.Read()

	require.Nil(t, result)
	require.NoError(t, err)
}

func Test_SyncReadConnection_ReadError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)
	protoClient.EXPECT().Recv().Return(nil, errors.New("some error"))

	syncReadConnection := newSyncReadConnection(protoClient, "some id", nil)

	result, err := syncReadConnection.Read()

	require.Nil(t, result)
	require.Equal(t, Read_FailedToRead_Err, err.(Error).Code())
}

func Test_SyncReadConnection_ReadEvent(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)
	messageAdapter := NewMockmessageAdapter(ctrl)

	response := &persistent.ReadResp{
		Content: &persistent.ReadResp_Event{},
	}
	expectedEvent := &messages.RecordedEvent{
		EventType: "some type",
	}

	protoClient.EXPECT().Recv().Return(response, nil)
	messageAdapter.EXPECT().FromProtoResponse(response).Return(expectedEvent)
	syncReadConnection := newSyncReadConnection(protoClient, "some id", messageAdapter)

	result, err := syncReadConnection.Read()

	require.Equal(t, expectedEvent, result)
	require.NoError(t, err)
}

func Test_SyncReadConnection_ErrReceiveSubscriptionConfirmation(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)

	response := &persistent.ReadResp{
		Content: &persistent.ReadResp_SubscriptionConfirmation_{},
	}

	protoClient.EXPECT().Recv().Return(response, nil)
	syncReadConnection := newSyncReadConnection(protoClient, "some id", nil)

	result, err := syncReadConnection.Read()

	require.Nil(t, result)
	require.Equal(t, Read_ReceivedSubscriptionConfirmation_Err, err.(Error).Code())
}

func Test_SyncReadConnection_Ack_NoMessages(t *testing.T) {
	syncReadConnection := syncReadConnectionImpl{}
	err := syncReadConnection.Ack()
	require.NoError(t, err)
}

func Test_SyncReadConnection_Ack_MaxMessageCountError(t *testing.T) {
	syncReadConnection := syncReadConnectionImpl{}

	messageIds := make([]uuid.UUID, 2001)
	for i := 0; i < 2001; i++ {
		messageIds[i], _ = uuid.NewV4()
	}

	err := syncReadConnection.Ack(messageIds...)
	require.Equal(t, Exceeds_Max_Message_Count_Err, err.(Error).Code())
}

func Test_SyncReadConnection_Ack_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)

	messageIds := make([]uuid.UUID, 200)
	for i := 0; i < 200; i++ {
		messageIds[i], _ = uuid.NewV4()
	}

	subscriptionId := "some subscription"
	expectedProtoRequest := &persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(subscriptionId),
				Ids: messageIdSliceToProto(messageIds...),
			},
		},
	}

	protoClient.EXPECT().Send(expectedProtoRequest).Return(nil)

	syncReadConnection := syncReadConnectionImpl{
		client:         protoClient,
		subscriptionId: subscriptionId,
		messageAdapter: nil,
	}
	err := syncReadConnection.Ack(messageIds...)
	require.NoError(t, err)
}

func Test_SyncReadConnection_Ack_SendError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)

	messageIds := make([]uuid.UUID, 200)
	for i := 0; i < 200; i++ {
		messageIds[i], _ = uuid.NewV4()
	}

	subscriptionId := "some subscription"
	expectedProtoRequest := &persistent.ReadReq{
		Content: &persistent.ReadReq_Ack_{
			Ack: &persistent.ReadReq_Ack{
				Id:  []byte(subscriptionId),
				Ids: messageIdSliceToProto(messageIds...),
			},
		},
	}

	errorResult := errors.New("some error")
	protoClient.EXPECT().Send(expectedProtoRequest).Return(errorResult)

	syncReadConnection := syncReadConnectionImpl{
		client:         protoClient,
		subscriptionId: subscriptionId,
		messageAdapter: nil,
	}
	err := syncReadConnection.Ack(messageIds...)
	require.Error(t, err)
}

func Test_SyncReadConnection_Nack_NoMessages(t *testing.T) {
	syncReadConnection := syncReadConnectionImpl{}
	err := syncReadConnection.Nack("some reason", Nack_Park)
	require.NoError(t, err)
}

func Test_SyncReadConnection_Nack_SendError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)

	messageIds := make([]uuid.UUID, 200)
	for i := 0; i < 200; i++ {
		messageIds[i], _ = uuid.NewV4()
	}

	subscriptionId := "some subscription"
	expectedAction := Nack_Park
	expectedReason := "some reason"
	expectedProtoRequest := &persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(subscriptionId),
				Ids:    messageIdSliceToProto(messageIds...),
				Action: persistent.ReadReq_Nack_Action(expectedAction),
				Reason: expectedReason,
			},
		},
	}

	errorResult := errors.New("some error")
	protoClient.EXPECT().Send(expectedProtoRequest).Return(errorResult)

	syncReadConnection := syncReadConnectionImpl{
		client:         protoClient,
		subscriptionId: subscriptionId,
		messageAdapter: nil,
	}
	err := syncReadConnection.Nack(expectedReason, expectedAction, messageIds...)
	require.Error(t, err)
}

func Test_SyncReadConnection_Nack_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	protoClient := NewMockprotoClient(ctrl)

	messageIds := make([]uuid.UUID, 200)
	for i := 0; i < 200; i++ {
		messageIds[i], _ = uuid.NewV4()
	}

	subscriptionId := "some subscription"
	expectedAction := Nack_Park
	expectedReason := "some reason"
	expectedProtoRequest := &persistent.ReadReq{
		Content: &persistent.ReadReq_Nack_{
			Nack: &persistent.ReadReq_Nack{
				Id:     []byte(subscriptionId),
				Ids:    messageIdSliceToProto(messageIds...),
				Action: persistent.ReadReq_Nack_Action(expectedAction),
				Reason: expectedReason,
			},
		},
	}

	protoClient.EXPECT().Send(expectedProtoRequest).Return(nil)

	syncReadConnection := syncReadConnectionImpl{
		client:         protoClient,
		subscriptionId: subscriptionId,
		messageAdapter: nil,
	}
	err := syncReadConnection.Nack(expectedReason, expectedAction, messageIds...)
	require.NoError(t, err)
}
