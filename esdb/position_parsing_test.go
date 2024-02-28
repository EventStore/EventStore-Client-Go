package esdb_test

import (
	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPositionParsing(t *testing.T) {
	t.Run("StreamPositionTests", func(t *testing.T) {
		pos, err := esdb.ParseStreamPosition("C:123/P:456")
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		obj, err := esdb.ParseStreamPosition("C:-1/P:-1")
		assert.NoError(t, err)

		_, ok := obj.(esdb.End)
		assert.True(t, ok)

		obj, err = esdb.ParseStreamPosition("C:0/P:0")
		assert.NoError(t, err)

		_, ok = obj.(esdb.Start)
		assert.True(t, ok)

		obj, err = esdb.ParseStreamPosition("-1")
		assert.NoError(t, err)

		_, ok = obj.(esdb.End)
		assert.True(t, ok)

		obj, err = esdb.ParseStreamPosition("0")
		assert.NoError(t, err)

		_, ok = obj.(esdb.Start)
		assert.True(t, ok)

		obj, err = esdb.ParseStreamPosition("42")
		assert.NoError(t, err)

		value, ok := obj.(esdb.StreamRevision)
		assert.True(t, ok)
		assert.Equal(t, uint64(42), value.Value)
	})
}
