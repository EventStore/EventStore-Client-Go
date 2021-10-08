package event_streams_with_prepopulated_database

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func TestSubscribeToAll_WithFilterDeliversCorrectEvents(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	positionsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-positions-filtered-stream-194-e0-e30.json"))
	require.NoError(t, err)
	versionsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-versions-filtered-stream-194-e0-e30.json"))
	require.NoError(t, err)
	var positions []Position
	var versions []uint64
	err = json.Unmarshal(positionsContent, &positions)
	require.NoError(t, err)
	err = json.Unmarshal(versionsContent, &versions)
	require.NoError(t, err)

	receivedEvents := sync.WaitGroup{}
	receivedEvents.Add(len(positions))

	reader, err := client.SubscribeToFilteredStreamAll(
		context.Background(),
		event_streams.ReadPositionAllStart{},
		false,
		event_streams.Filter{
			FilterBy: event_streams.FilterByEventType{
				Matcher: event_streams.PrefixFilterMatcher{
					PrefixList: []string{"eventType-194"},
				},
			},
			Window:                       event_streams.DefaultFilterWindowMax(),
			CheckpointIntervalMultiplier: event_streams.DefaultCheckpointIntervalMultiplier,
		})
	require.NoError(t, err)

	go func() {
		current := 0
		for {
			if current == len(versions) {
				break
			}

			subEvent, err := reader.ReadOne()
			require.NoError(t, err)

			if event, isEvent := subEvent.GetEvent(); isEvent {
				require.Equal(t, versions[current], event.GetOriginalEvent().EventNumber)
				require.Equal(t, positions[current].Commit, event.GetOriginalEvent().Position.Commit)
				require.Equal(t, positions[current].Prepare, event.GetOriginalEvent().Position.Prepare)
				current++
				receivedEvents.Done()
			}
		}
	}()

	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out while waiting for events via the subscription")
}
