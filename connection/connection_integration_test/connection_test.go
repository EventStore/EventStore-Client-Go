package connection_integration_test

//func Test_CloseConnection(t *testing.T) {
//	container := getEmptyDatabase()
//	defer container.Close()
//
//	client := createClientConnectedToContainer(container, t)
//
//	testEvent := event_streams.ProposedEvent{
//		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
//		EventType:    "TestEvent",
//		ContentType:  "application/octet-stream",
//		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
//		Data:         []byte{0xb, 0xe, 0xe, 0xf},
//	}
//	proposedEvents := []event_streams.ProposedEvent{
//		testEvent,
//	}
//
//	streamID, _ := uuid.NewV4()
//	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//	_, err := client.EventStreams().AppendToStream(ctx,
//		streamID.String(),
//		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
//		proposedEvents)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	client.Close()
//	_, err = client.EventStreams().AppendToStream(ctx,
//		streamID.String(),
//		event_streams.AppendRequestExpectedStreamRevisionAny{},
//		proposedEvents)
//
//	assert.NotNil(t, err)
//	assert.Equal(t, connection.EsdbConnectionIsClosed, err.Code())
//}
