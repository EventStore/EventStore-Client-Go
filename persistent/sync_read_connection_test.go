package persistent

import (
	"errors"
	"io"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
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
