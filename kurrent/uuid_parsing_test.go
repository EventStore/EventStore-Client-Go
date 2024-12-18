package kurrent_test

import (
	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func UUIDParsingTests(t *testing.T) {
	t.Run("UUIDParsingTests", func(t *testing.T) {
		expected := uuid.New()
		most, least := kurrent.UUIDAsInt64(expected)
		actual, err := kurrent.ParseUUIDFromInt64(most, least)

		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}
