package event_streams

import (
	"testing"

	"github.com/google/uuid"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
	"github.com/stretchr/testify/require"
)

func TestProposedEvent_ToBatchMessage(t *testing.T) {
	eventId, _ := uuid.NewRandom()
	event := ProposedEvent{
		EventId:      eventId,
		EventType:    "some type",
		ContentType:  "content type",
		Data:         []byte("some data"),
		UserMetadata: []byte("some user metadata"),
	}

	result := event.toBatchMessage()

	expectedBatchMessage := batchAppendRequestProposedMessage{
		Id: eventId,
		Metadata: map[string]string{
			system_metadata.SystemMetadataKeysContentType: "content type",
			system_metadata.SystemMetadataKeysType:        "some type",
		},
		CustomMetadata: []byte("some user metadata"),
		Data:           []byte("some data"),
	}
	require.Equal(t, expectedBatchMessage, result)
}

func TestProposedEvent_toBatchAppendRequestList(t *testing.T) {
	eventId, _ := uuid.NewRandom()
	eventId2, _ := uuid.NewRandom()
	eventList := ProposedEventList{
		{
			EventId:      eventId,
			EventType:    "some type",
			ContentType:  "content type",
			Data:         []byte("some data"),
			UserMetadata: []byte("some user metadata"),
		},
		{
			EventId:      eventId2,
			EventType:    "some type 2",
			ContentType:  "content type 2",
			Data:         []byte("some data 2"),
			UserMetadata: []byte("some user metadata 2"),
		},
	}

	result := eventList.toBatchAppendRequestList()

	expectedBatchMessages := []batchAppendRequestProposedMessage{
		{
			Id: eventId,
			Metadata: map[string]string{
				system_metadata.SystemMetadataKeysContentType: "content type",
				system_metadata.SystemMetadataKeysType:        "some type",
			},
			CustomMetadata: []byte("some user metadata"),
			Data:           []byte("some data"),
		},
		{
			Id: eventId2,
			Metadata: map[string]string{
				system_metadata.SystemMetadataKeysContentType: "content type 2",
				system_metadata.SystemMetadataKeysType:        "some type 2",
			},
			CustomMetadata: []byte("some user metadata 2"),
			Data:           []byte("some data 2"),
		},
	}

	require.Equal(t, expectedBatchMessages, result)
}

func TestProposedEvent_toBatchAppendRequestChunks(t *testing.T) {
	eventId, _ := uuid.NewRandom()
	eventId2, _ := uuid.NewRandom()
	eventId3, _ := uuid.NewRandom()
	eventId4, _ := uuid.NewRandom()
	eventId5, _ := uuid.NewRandom()
	eventId6, _ := uuid.NewRandom()
	eventList := ProposedEventList{
		{
			EventId:      eventId,
			EventType:    "some type",
			ContentType:  "content type",
			Data:         []byte("some data"),
			UserMetadata: []byte("some user metadata"),
		},
		{
			EventId:      eventId2,
			EventType:    "some type 2",
			ContentType:  "content type 2",
			Data:         []byte("some data 2"),
			UserMetadata: []byte("some user metadata 2"),
		},
		{
			EventId:      eventId3,
			EventType:    "some type 3",
			ContentType:  "content type 3",
			Data:         []byte("some data 3"),
			UserMetadata: []byte("some user metadata 3"),
		},
		{
			EventId:      eventId4,
			EventType:    "some type 4",
			ContentType:  "content type 4",
			Data:         []byte("some data 4"),
			UserMetadata: []byte("some user metadata 4"),
		},
		{
			EventId:      eventId5,
			EventType:    "some type 5",
			ContentType:  "content type 5",
			Data:         []byte("some data 5"),
			UserMetadata: []byte("some user metadata 5"),
		},
		{
			EventId:      eventId6,
			EventType:    "some type 6",
			ContentType:  "content type 6",
			Data:         []byte("some data 6"),
			UserMetadata: []byte("some user metadata 6"),
		},
	}

	t.Run("Chunk size 0", func(t *testing.T) {
		require.Panics(t, func() {
			eventList.toBatchAppendRequestChunks(0)
		})
	})

	t.Run("Chunk size 1", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
			},
			{
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
			},
			{
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
			},
			{
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
			},
			{
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
			},
			{
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(1)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 2", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
			},
			{
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
			},
			{
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(2)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 3", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
			},
			{
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(3)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 4", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
			},
			{
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(4)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 5", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
			},
			{
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(5)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 6", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(6)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Chunk size 6", func(t *testing.T) {
		expectedBatchMessages := [][]batchAppendRequestProposedMessage{
			{
				{
					Id: eventId,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type",
						system_metadata.SystemMetadataKeysType:        "some type",
					},
					CustomMetadata: []byte("some user metadata"),
					Data:           []byte("some data"),
				},
				{
					Id: eventId2,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 2",
						system_metadata.SystemMetadataKeysType:        "some type 2",
					},
					CustomMetadata: []byte("some user metadata 2"),
					Data:           []byte("some data 2"),
				},
				{
					Id: eventId3,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 3",
						system_metadata.SystemMetadataKeysType:        "some type 3",
					},
					CustomMetadata: []byte("some user metadata 3"),
					Data:           []byte("some data 3"),
				},
				{
					Id: eventId4,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 4",
						system_metadata.SystemMetadataKeysType:        "some type 4",
					},
					CustomMetadata: []byte("some user metadata 4"),
					Data:           []byte("some data 4"),
				},
				{
					Id: eventId5,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 5",
						system_metadata.SystemMetadataKeysType:        "some type 5",
					},
					CustomMetadata: []byte("some user metadata 5"),
					Data:           []byte("some data 5"),
				},
				{
					Id: eventId6,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysContentType: "content type 6",
						system_metadata.SystemMetadataKeysType:        "some type 6",
					},
					CustomMetadata: []byte("some user metadata 6"),
					Data:           []byte("some data 6"),
				},
			},
		}

		result := eventList.toBatchAppendRequestChunks(7)
		require.Equal(t, expectedBatchMessages, result)
	})

	t.Run("Input slice is empty", func(t *testing.T) {
		result := ProposedEventList{}.toBatchAppendRequestChunks(1)
		require.Len(t, result, 1)
		require.Equal(t, []batchAppendRequestProposedMessage{}, result[0])
	})
}
