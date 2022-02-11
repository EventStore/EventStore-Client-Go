package esdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPositionParsing(t *testing.T) {
	pos, err := parseStreamPosition("C:123/P:456")
	assert.NoError(t, err)
	assert.NotNil(t, pos)

	obj, err := parseStreamPosition("C:-1/P:-1")
	assert.NoError(t, err)

	_, ok := obj.(End)
	assert.True(t, ok)

	obj, err = parseStreamPosition("C:0/P:0")
	assert.NoError(t, err)

	_, ok = obj.(Start)
	assert.True(t, ok)

	obj, err = parseStreamPosition("-1")
	assert.NoError(t, err)

	_, ok = obj.(End)
	assert.True(t, ok)

	obj, err = parseStreamPosition("0")
	assert.NoError(t, err)

	_, ok = obj.(Start)
	assert.True(t, ok)

	obj, err = parseStreamPosition("42")
	assert.NoError(t, err)

	value, ok := obj.(StreamRevision)
	assert.True(t, ok)
	assert.Equal(t, uint64(42), value.Value)
}
