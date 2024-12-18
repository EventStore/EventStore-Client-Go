package kurrent_test

import (
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/stretchr/testify/assert"
)

func TestConsistentMetadataSerializationStreamAcl(t *testing.T) {
	acl := kurrent.Acl{}
	acl.AddReadRoles("admin")
	acl.AddWriteRoles("admin")
	acl.AddDeleteRoles("admin")
	acl.AddMetaReadRoles("admin")
	acl.AddMetaWriteRoles("admin")

	expected := kurrent.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(acl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := kurrent.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationUserStreamAcl(t *testing.T) {
	expected := kurrent.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(kurrent.UserStreamAcl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := kurrent.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationSystemStreamAcl(t *testing.T) {
	expected := kurrent.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(kurrent.SystemStreamAcl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := kurrent.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}

func TestCustomPropertyRetrievalFromStreamMetadata(t *testing.T) {
	expected := kurrent.StreamMetadata{}
	expected.AddCustomProperty("foo", "bar")

	foo := expected.CustomProperty("foo")

	assert.Equal(t, "bar", foo, "custom property value mismatch")
}

func TestUnknownCustomPropertyRetrievalFromStreamMetadata(t *testing.T) {
	expected := kurrent.StreamMetadata{}
	expected.AddCustomProperty("foo", "bar")

	foo := expected.CustomProperty("foes")

	assert.Empty(t, foo, "custom property value mismatch")
}

func TestGetAllCustomPropertiesFromStreamMetadata(t *testing.T) {
	expected := kurrent.StreamMetadata{}
	expected.AddCustomProperty("foo", 123)
	expected.AddCustomProperty("foes", "baz")

	props := expected.CustomProperties()

	assert.Equal(t, map[string]interface{}{"foo": 123, "foes": "baz"}, props, "custom properties mismatch")
}
