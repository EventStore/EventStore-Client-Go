package client_test

//func TestCanDeleteStream(t *testing.T) {
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	deleteResult, err := client.DeleteStream_OLD(context.Background(), "dataset20M-1800", streamrevision.NewStreamRevision(1999))
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	assert.True(t, deleteResult.Position.Commit > 0)
//	assert.True(t, deleteResult.Position.Prepare > 0)
//}

//func TestCanTombstoneStream(t *testing.T) {
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	deleteResult, err := client.TombstoneStream_OLD(context.Background(), "dataset20M-1800", streamrevision.NewStreamRevision(1999))
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	assert.True(t, deleteResult.Position.Commit > 0)
//	assert.True(t, deleteResult.Position.Prepare > 0)
//
//	_, err = client.AppendToStream_OLD(context.Background(), "dataset20M-1800", streamrevision.StreamRevisionAny, []messages.ProposedEvent{createTestEvent()})
//	require.Error(t, err)
//}
