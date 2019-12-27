package esgrpc_test

import (
	"testing"

	"github.com/eventstore/EventStore-Client-Go/esgrpc"
	uuid "github.com/gofrs/uuid"
)

func createTestClient(t *testing.T) *esgrpc.Client {
	config := esgrpc.NewConfiguration()
	config.Address = "localhost:2113"
	config.Username = "admin"
	config.Password = "changeit"
	config.SkipCertificateVerification = true

	client, err := esgrpc.NewClient(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}
	err = client.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}
	return client
}

func createTestMessage() esgrpc.Message {
	return esgrpc.Message{
		EventID:   uuid.Must(uuid.NewV4()),
		EventType: "TestEvent",
		IsJSON:    true,
		Data:      []byte("{}"),
		Metadata:  map[string]string{},
	}
}

func TestAppendToStream_SingleEvent(t *testing.T) {
	client := createTestClient(t)
	defer client.Close()
	messages := []esgrpc.Message{
		createTestMessage(),
	}

	streamID, _ := uuid.NewV4()
	_, err := client.AppendToStream(streamID.String(), esgrpc.StreamRevisionAny, messages)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
}
