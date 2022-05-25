package esdb_test

import (
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/assert"
)

func TestConsistentMetadataSerializationStreamAcl(t *testing.T) {
	acl := esdb.Acl{}
	acl.AddReadRoles("admin")
	acl.AddWriteRoles("admin")
	acl.AddDeleteRoles("admin")
	acl.AddMetaReadRoles("admin")
	acl.AddMetaWriteRoles("admin")

	expected := esdb.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(acl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := esdb.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationUserStreamAcl(t *testing.T) {
	expected := esdb.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(esdb.UserStreamAcl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := esdb.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}

func TestConsistentMetadataSerializationSystemStreamAcl(t *testing.T) {
	expected := esdb.StreamMetadata{}
	expected.SetMaxAge(2 * time.Second)
	expected.SetCacheControl(15 * time.Second)
	expected.SetTruncateBefore(1)
	expected.SetMaxCount(12)
	expected.SetAcl(esdb.SystemStreamAcl)
	expected.AddCustomProperty("foo", "bar")

	bytes, err := expected.ToJson()

	assert.NoError(t, err, "failed to serialize in JSON")

	meta, err := esdb.StreamMetadataFromJson(bytes)

	assert.NoError(t, err, "failed to parse Metadata from props")

	assert.Equal(t, expected, *meta, "consistency serialization failure")
}
