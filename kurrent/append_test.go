package kurrent_test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"io"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/stretchr/testify/assert"
)

func createTestEvent() kurrent.EventData {
	event := kurrent.EventData{
		EventType:   "TestEvent",
		ContentType: kurrent.ContentTypeBinary,
		EventID:     uuid.New(),
		Data:        []byte{0xb, 0xe, 0xe, 0xf},
		Metadata:    []byte{0xd, 0xe, 0xa, 0xd},
	}

	return event
}

func collectStreamEvents(stream *kurrent.ReadStream) ([]*kurrent.ResolvedEvent, error) {
	events := []*kurrent.ResolvedEvent{}

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

func AppendTests(t *testing.T, emptyDB *Container, emptyDBClient *kurrent.Client) {
	t.Run("AppendTests", func(t *testing.T) {
		t.Run("appendToStreamSingleEventNoStream", appendToStreamSingleEventNoStream(emptyDBClient))
		t.Run("appendWithInvalidStreamRevision", appendWithInvalidStreamRevision(emptyDBClient))
		t.Run("appendToSystemStreamWithIncorrectCredentials", appendToSystemStreamWithIncorrectCredentials(emptyDB))
		t.Run("metadataOperation", metadataOperation(emptyDBClient))
	})
}

func appendToStreamSingleEventNoStream(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		testEvent := createTestEvent()
		testEvent.EventID = uuid.MustParse("38fffbc2-339e-11ea-8c7b-784f43837872")

		streamID := uuid.New()
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrent.AppendToStreamOptions{
			ExpectedRevision: kurrent.NoStream{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, testEvent)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		stream, err := db.ReadStream(context, streamID.String(), kurrent.ReadStreamOptions{}, 1)

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

func appendWithInvalidStreamRevision(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		streamID := uuid.New()
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrent.AppendToStreamOptions{
			ExpectedRevision: kurrent.StreamExists{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, createTestEvent())
		esdbErr, ok := kurrent.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), kurrent.ErrorCodeWrongExpectedVersion)
	}
}

func appendToSystemStreamWithIncorrectCredentials(container *Container) TestCall {
	return func(t *testing.T) {
		isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

		if container == nil || isInsecure {
			t.Skip()
		}

		conn := fmt.Sprintf("esdb://bad_user:bad_password@%s?tlsverifycert=false", container.Endpoint)
		config, err := kurrent.ParseConnectionString(conn)
		if err != nil {
			t.Fatalf("Unexpected configuration error: %s", err.Error())
		}

		db, err := kurrent.NewClient(config)
		if err != nil {
			t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
		}

		defer db.Close()

		streamID := uuid.New()
		context, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		opts := kurrent.AppendToStreamOptions{
			ExpectedRevision: kurrent.Any{},
		}

		_, err = db.AppendToStream(context, streamID.String(), opts, createTestEvent())
		esdbErr, ok := kurrent.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), kurrent.ErrorCodeUnauthenticated)
	}
}

func metadataOperation(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		streamID := uuid.New()
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrent.AppendToStreamOptions{
			ExpectedRevision: kurrent.Any{},
		}

		_, err := db.AppendToStream(context, streamID.String(), opts, createTestEvent())

		assert.Nil(t, err, "error when writing an event")

		acl := kurrent.Acl{}
		acl.AddReadRoles("admin")

		meta := kurrent.StreamMetadata{}
		meta.SetMaxAge(2 * time.Second)
		meta.SetAcl(acl)

		result, err := db.SetStreamMetadata(context, streamID.String(), opts, meta)

		assert.Nil(t, err, "no error from writing stream metadata")
		assert.NotNil(t, result, "defined write result after writing metadata")

		metaActual, err := db.GetStreamMetadata(context, streamID.String(), kurrent.ReadStreamOptions{})

		assert.Nil(t, err, "no error when reading stream metadata")

		assert.Equal(t, meta, *metaActual, "matching metadata")
	}
}
