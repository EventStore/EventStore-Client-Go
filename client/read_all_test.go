package client_test

//func TestReadAllEventsForwardsFromZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-e0-e10.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Forwards, stream_position.Start{}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsForwardsFromNonZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-c1788-p1788.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Forwards, stream_position.Position{Value: position.Position{Commit: 1788, Prepare: 1788}}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsBackwardsFromZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-e0-e10.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Backwards, stream_position.End{}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsBackwardsFromNonZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-c3386-p3386.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Backwards, stream_position.Position{Value: position.Position{Commit: 3386, Prepare: 3386}}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
