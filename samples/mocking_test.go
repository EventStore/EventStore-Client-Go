package samples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/mock"
)

// Mocks

type MockIReadStream struct {
	mock.Mock
}

func (m *MockIReadStream) Close() { m.Called() }

func (m *MockIReadStream) Recv() (*esdb.ResolvedEvent, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*esdb.ResolvedEvent), args.Error(1)
}

type IEventStoreClient interface {
	AppendToStream(context context.Context, streamID string, opts esdb.AppendToStreamOptions, events ...esdb.EventData) (*esdb.WriteResult, error)
	ReadStream(context context.Context, streamID string, opts esdb.ReadStreamOptions, count uint64) (esdb.IReadStream, error)
}

type MockIEventStoreClient struct {
	mock.Mock
}

func (m *MockIEventStoreClient) AppendToStream(context context.Context, streamID string, opts esdb.AppendToStreamOptions, events ...esdb.EventData) (*esdb.WriteResult, error) {
	args := m.Called(context, streamID, opts, events)
	return args.Get(0).(*esdb.WriteResult), args.Error(1)
}

func (m *MockIEventStoreClient) ReadStream(context context.Context, streamID string, opts esdb.ReadStreamOptions, count uint64) (esdb.IReadStream, error) {
	args := m.Called(context, streamID, opts, count)
	return args.Get(0).(esdb.IReadStream), args.Error(1)
}

// Logic

func TestMockingReadStream(t *testing.T) {
	// Arrange
	fakeEvents := generateFakeEvents()
	mockResolvedEvents := mapToResolvedEvents(fakeEvents)
	mockReadStream := new(MockIReadStream)
	for _, v := range mockResolvedEvents {
		mockReadStream.On("Recv").Return(v, nil).Once()
	}
	mockReadStream.On("Recv").Return(nil, io.EOF).Once()

	mockEventStoreClient := new(MockIEventStoreClient)
	mockEventStoreClient.On("ReadStream", context.Background(), "FakeStreamID", esdb.ReadStreamOptions{}, uint64(200)).Return(mockReadStream, nil)

	// Act
	logicToTest(mockEventStoreClient)

	// Assert
	mockReadStream.AssertExpectations(t)
	mockEventStoreClient.AssertExpectations(t)
}

func logicToTest(eventStoreClient IEventStoreClient) {

	//.. something to test

	stream, _ := eventStoreClient.ReadStream(context.Background(), "FakeStreamID", esdb.ReadStreamOptions{}, 200)
	for {
		eventData, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		//do something with eventData
		print(eventData)
	}

	//.. something to test

}

type FakeEvent struct {
	EventID uuid.UUID
	Data    string
}

// Helpers

func generateFakeEvents() (events []FakeEvent) {
	for i := 0; i < 5; i++ {
		event := FakeEvent{
			EventID: generateNewUUID(),
			Data:    "Number " + fmt.Sprint(i),
		}
		events = append(events, event)
	}
	return
}

func mapToResolvedEvents(events []FakeEvent) (resolvedEvents []*esdb.ResolvedEvent) {
	for _, event := range events {
		resolvedEvent := &esdb.ResolvedEvent{
			Event: &esdb.RecordedEvent{
				EventID: event.EventID,
				Data:    []byte(event.Data),
			},
		}
		resolvedEvents = append(resolvedEvents, resolvedEvent)
	}
	return
}

func generateNewUUID() uuid.UUID {
	id, _ := uuid.NewV4()
	return id
}
