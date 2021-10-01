package projections

import (
	stdErrors "errors"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"google.golang.org/grpc/metadata"

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

	t.Run("Error returned from proto client does not match known exception", func(t *testing.T) {
		errorResult := stdErrors.New("some error")

		protoClient := projections.NewMockProjections_StatisticsClient(ctrl)
		protoClient.EXPECT().Recv().Return(nil, errorResult)
		protoClient.EXPECT().Trailer().Return(metadata.MD{})

		statisticsClient := newStatisticsClientSyncImpl(protoClient)
		result, err := statisticsClient.Read()

		require.Error(t, err)
		require.Equal(t, errors.FatalError, err.Code())
		require.Equal(t, StatisticsClientResponse{}, result)
	})

	t.Run("Error returned from proto client matches known exception", func(t *testing.T) {
		errorResult := stdErrors.New("some error")

		protoClient := projections.NewMockProjections_StatisticsClient(ctrl)
		protoClient.EXPECT().Recv().Return(nil, errorResult)
		protoClient.EXPECT().Trailer().Return(metadata.MD{"exception": []string{"maximum-subscribers-reached"}})

		statisticsClient := newStatisticsClientSyncImpl(protoClient)
		result, err := statisticsClient.Read()

		require.Error(t, err)
		require.Equal(t, errors.MaximumSubscriberCountReached, err.Code())
		require.Equal(t, StatisticsClientResponse{}, result)
	})
}
