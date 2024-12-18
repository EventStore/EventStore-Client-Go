package kurrent_test

import (
	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPositionParsing(t *testing.T) {
	t.Run("StreamPositionTests", func(t *testing.T) {
		pos, err := kurrent.ParseStreamPosition("C:123/P:456")
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		obj, err := kurrent.ParseStreamPosition("C:-1/P:-1")
		assert.NoError(t, err)

		_, ok := obj.(kurrent.End)
		assert.True(t, ok)

		obj, err = kurrent.ParseStreamPosition("C:0/P:0")
		assert.NoError(t, err)

		_, ok = obj.(kurrent.Start)
		assert.True(t, ok)

		obj, err = kurrent.ParseStreamPosition("-1")
		assert.NoError(t, err)

		_, ok = obj.(kurrent.End)
		assert.True(t, ok)

		obj, err = kurrent.ParseStreamPosition("0")
		assert.NoError(t, err)

		_, ok = obj.(kurrent.Start)
		assert.True(t, ok)

		obj, err = kurrent.ParseStreamPosition("42")
		assert.NoError(t, err)

		value, ok := obj.(kurrent.StreamRevision)
		assert.True(t, ok)
		assert.Equal(t, uint64(42), value.Value)
	})
}
