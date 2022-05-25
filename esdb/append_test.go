package esdb_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() esdb.EventData {
	event := esdb.EventData{
		EventType:   "TestEvent",
		ContentType: esdb.ContentTypeBinary,
		EventID:     uuid.Must(uuid.NewV4()),
		Data:        []byte{0xb, 0xe, 0xe, 0xf},
		Metadata:    []byte{0xd, 0xe, 0xa, 0xd},
	}

	return event
}

func collectStreamEvents(stream *esdb.ReadStream) ([]*esdb.ResolvedEvent, error) {
	events := []*esdb.ResolvedEvent{}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		events = append(events, event)
	}

	return events, nil
}

type TestCall = func(t *testing.T)

func AppendTests(t *testing.T, emptyDB *Container, emptyDBClient *esdb.Client) {
	t.Run("AppendTests", func(t *testing.T) {
		t.Run("appendToStreamSingleEventNoStream", appendToStreamSingleEventNoStream(emptyDBClient))
		t.Run("appendWithInvalidStreamRevision", appendWithInvalidStreamRevision(emptyDBClient))
		t.Run("appendToSystemStreamWithIncorrectCredentials", appendToSystemStreamWithIncorrectCredentials(emptyDB))
		t.Run("metadataOperation", metadataOperation(emptyDBClient))
	})
}

func appendToStreamSingleEventNoStream(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		testEvent := createTestEvent()
		testEvent.EventID = uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872")

		streamID := uuid.Must(uuid.NewV4())
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.NoStream{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, testEvent)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		stream, err := db.ReadStream(context, streamID.String(), esdb.ReadStreamOptions{}, 1)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		events, err := collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, int32(1), int32(len(events)), "Expected the correct number of messages to be returned")
		assert.Equal(t, testEvent.EventID, events[0].OriginalEvent().EventID)
		assert.Equal(t, testEvent.EventType, events[0].OriginalEvent().EventType)
		assert.Equal(t, streamID.String(), events[0].OriginalEvent().StreamID)
		assert.Equal(t, testEvent.Data, events[0].OriginalEvent().Data)
		assert.Equal(t, testEvent.Metadata, events[0].OriginalEvent().UserMetadata)
	}
}

func appendWithInvalidStreamRevision(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID, _ := uuid.NewV4()
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.StreamExists{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, createTestEvent())
		esdbErr, ok := esdb.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), esdb.ErrorCodeWrongExpectedVersion)
	}
}

func appendToSystemStreamWithIncorrectCredentials(container *Container) TestCall {
	return func(t *testing.T) {
		if container == nil {
			t.Skip()
		}

		conn := fmt.Sprintf("esdb://bad_user:bad_password@%s?tlsverifycert=false", container.Endpoint)
		config, err := esdb.ParseConnectionString(conn)
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		db, err := esdb.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
		}

		defer db.Close()

		streamID, _ := uuid.NewV4()
		context, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		opts := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.Any{},
		}

		_, err = db.AppendToStream(context, streamID.String(), opts, createTestEvent())
		esdbErr, ok := esdb.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), esdb.ErrorCodeUnauthenticated)
	}
}

func metadataOperation(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := uuid.Must(uuid.NewV4())
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.Any{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, createTestEvent())

		assert.Nil(t, err, "error when writing an event")

		acl := esdb.Acl{}
		acl.AddReadRoles("admin")

		meta := esdb.StreamMetadata{}
		meta.SetMaxAge(2 * time.Second)
		meta.SetAcl(acl)

		result, err := db.SetStreamMetadata(context, streamID.String(), opts, meta)

		assert.Nil(t, err, "no error from writing stream metadata")
		assert.NotNil(t, result, "defined write result after writing metadata")

		metaActual, err := db.GetStreamMetadata(context, streamID.String(), esdb.ReadStreamOptions{})

		assert.Nil(t, err, "no error when reading stream metadata")

		assert.Equal(t, meta, *metaActual, "matching metadata")
	}
}
