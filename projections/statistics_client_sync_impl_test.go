package projections

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

func TestStatisticsClientSyncImpl_Read(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Result with no error returned from proto client", func(t *testing.T) {
		protoResult := &projections.StatisticsResp{
			Details: &projections.StatisticsResp_Details{
				Name: "some name",
			},
		}

		protoClient := projections.NewMockProjections_StatisticsClient(ctrl)
		protoClient.EXPECT().Recv().Return(protoResult, nil)

		statisticsClient := newStatisticsClientSyncImpl(protoClient)
		result, err := statisticsClient.Read()

		require.NoError(t, err)
		require.Equal(t, StatisticsClientResponse{Name: "some name"}, result)
	})

	t.Run("Error returned from proto client", func(t *testing.T) {
		errorResult := errors.New("some error")

		protoClient := projections.NewMockProjections_StatisticsClient(ctrl)
		protoClient.EXPECT().Recv().Return(nil, errorResult)

		statisticsClient := newStatisticsClientSyncImpl(protoClient)
		result, err := statisticsClient.Read()

		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
		require.Equal(t, StatisticsClientResponse{}, result)
	})
}
