package kurrent_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/stretchr/testify/require"
)

func ProjectionTests(t *testing.T, emptyDBClient *kurrent.Client) {
	t.Run("ProjectionTests", func(t *testing.T) {
		projClient := kurrent.NewProjectionClientFromExistingClient(emptyDBClient)

		t.Run("createProjection", createProjection(projClient))
		t.Run("deleteProjection", deleteProjection(projClient))
		t.Run("updateProjection", updateProjection(projClient))
		t.Run("enableProjection", enableProjection(projClient))
		t.Run("disableProjection", disableProjection(projClient))
		t.Run("resetProjection", resetProjection(projClient))
		t.Run("getStateProjection", getStateProjection(projClient))
		t.Run("getResultProjection", getResultProjection(projClient))
	})
}

func createProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")
	}
}

func deleteProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		err = client.Disable(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Stopped")

		// This race-condition is still happening even if it's marked as fixed: https://github.com/EventStore/EventStore/issues/2938
		// For the previous-lts version date: 2024-02-20 (22.10)
		// As result we still have to use some mitigation to make the delete call successful
		done := make(chan bool)
		go func() {
			for {
				err = client.Delete(context.Background(), name, kurrent.DeleteProjectionOptions{})

				if esdbErr, ok := kurrent.FromError(err); !ok {
					if !esdbErr.IsErrorCode(kurrent.ErrorCodeUnknown) {
						t.Errorf("error when deleting projection '%s': %v", name, esdbErr)
					}

					time.Sleep(500 * time.Millisecond)
					continue
				}

				break
			}

			done <- true
		}()

		select {
		case <-done:
			return
		case <-time.After(1 * time.Minute):
			t.Errorf("failed to delete projection '%s' in a timely manner", name)
		}
	}
}

func updateProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})

		// It seems we experience some flakiness on previous-lts version when running in secure mode.
		// It's most likely https://github.com/EventStore/EventStore/issues/2938 acting up again.
		// Date 2024-02-21
		for i := 0; i < 100; i++ {
			if esdbErr, ok := kurrent.FromError(err); !ok {
				if esdbErr.IsErrorCode(kurrent.ErrorCodeUnknown) {
					time.Sleep(1 * time.Second)
					err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
					continue
				}
			}

			break
		}

		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		scriptUpdated, err := os.ReadFile("../resources/test/projection-updated.js")

		err = client.Update(context.Background(), name, string(scriptUpdated), kurrent.UpdateProjectionOptions{})
		require.NoError(t, err)

		status, err := client.GetStatus(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		require.Equal(t, status.Name, name)
		require.Equal(t, status.Version, int64(1))

	}
}

func enableProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		err = client.Enable(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")
	}
}

func disableProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		err = client.Enable(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		err = client.Abort(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Stopped")
	}
}

func resetProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		script, err := os.ReadFile("../resources/test/projection.js")
		require.NoError(t, err)
		name := NAME_GENERATOR.Generate()

		err = client.Create(context.Background(), name, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, name, "Running")

		err = client.Enable(context.Background(), name, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		err = client.Reset(context.Background(), name, kurrent.ResetProjectionOptions{})
		require.NoError(t, err)
	}
}

func getStateProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		var events []kurrent.EventData

		for i := 0; i < 10; i++ {
			events = append(events, createTestEvent())
		}

		streamName := NAME_GENERATOR.Generate()
		script, err := os.ReadFile("../resources/test/projection.js")
		projName := NAME_GENERATOR.Generate()
		_, err = client.Client().AppendToStream(context.Background(), streamName, kurrent.AppendToStreamOptions{}, events...)
		require.NoError(t, err)

		err = client.Create(context.Background(), projName, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, projName, "Running")
		err = client.Enable(context.Background(), projName, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilStateReady(t, 5*time.Minute, client, projName)
	}
}

func getResultProjection(client *kurrent.ProjectionClient) TestCall {
	return func(t *testing.T) {
		var events []kurrent.EventData

		for i := 0; i < 10; i++ {
			events = append(events, createTestEvent())
		}

		streamName := NAME_GENERATOR.Generate()
		script, err := os.ReadFile("../resources/test/projection.js")
		projName := NAME_GENERATOR.Generate()
		_, err = client.Client().AppendToStream(context.Background(), streamName, kurrent.AppendToStreamOptions{}, events...)
		require.NoError(t, err)

		err = client.Create(context.Background(), projName, string(script), kurrent.CreateProjectionOptions{})
		require.NoError(t, err)

		waitUntilProjectionStatusIs(t, 5*time.Minute, client, projName, "Running")
		err = client.Enable(context.Background(), projName, kurrent.GenericProjectionOptions{})
		require.NoError(t, err)

		waitUntilResultReady(t, 5*time.Minute, client, projName)
	}
}

func waitUntilProjectionStatusIs(t *testing.T, duration time.Duration, client *kurrent.ProjectionClient, name string, status string) {
	done := make(chan bool)

	go func() {
		for {
			stats, err := client.GetStatus(context.Background(), name, kurrent.GenericProjectionOptions{})
			if err != nil {
				t.Errorf("error when reading projection '%s' stats: %v", name, err)
			}

			if strings.Contains(stats.Status, status) {
				break
			}

			t.Logf("waiting for projection '%s' status to be '%s' but was '%s' instead. retrying...", name, status, stats.Status)
			time.Sleep(1 * time.Second)
		}

		done <- true
	}()

	select {
	case <-done:
		return
	case <-time.After(duration):
		t.Errorf("projection '%s' failed to reach '%s' status in a timely manner", name, status)
	}
}

func waitUntilStateReady(t *testing.T, duration time.Duration, client *kurrent.ProjectionClient, name string) {
	done := make(chan *state)
	go func() {
		for {
			result, err := client.GetState(context.Background(), name, kurrent.GetStateProjectionOptions{})

			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var state state
			content, err := result.MarshalJSON()

			if err != nil {
				t.Errorf("error when unmarshalling protobuf types to json: %v", err)
			}

			err = json.Unmarshal(content, &state)
			if err != nil {
				t.Errorf("error when unmarshalling projection internal state type: %v", err)
			}

			done <- &state
			return
		}
	}()

	select {
	case <-done:
		return
	case <-time.After(duration):
		t.Errorf("unable to get projection '%s' internal state in a timely manner", name)
	}
}

func waitUntilResultReady(t *testing.T, duration time.Duration, client *kurrent.ProjectionClient, name string) {
	done := make(chan *state)
	go func() {
		for {
			result, err := client.GetResult(context.Background(), name, kurrent.GetResultProjectionOptions{})

			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var state state
			content, err := result.MarshalJSON()

			if err != nil {
				t.Errorf("error when unmarshalling protobuf types to json: %v", err)
			}

			err = json.Unmarshal(content, &state)
			if err != nil {
				t.Errorf("error when unmarshalling projection internal result type: %v", err)
			}

			done <- &state
			return
		}
	}()

	select {
	case <-done:
		return
	case <-time.After(duration):
		t.Errorf("unable to get projection '%s' internal state in a timely manner", name)
	}
}

type state struct {
	Foo foo `json:"foo"`
}

type foo struct {
	Baz baz `json:"baz"`
}

type baz struct {
	Count float64 `json:"count"`
}
