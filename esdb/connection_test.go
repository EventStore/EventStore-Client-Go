package esdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_CloseConnection(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)

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

	db.Close()
	opts.ExpectedRevision = esdb.Any{}
	_, err = db.AppendToStream(context, streamID.String(), opts, testEvent)

	assert.NotNil(t, err)
	assert.Equal(t, "can't get a connection handle: esdb connection is closed", err.Error())
}
