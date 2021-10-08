package protobuf_uuid

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_ConstructAndReconstructUUID(t *testing.T) {
	id, _ := uuid.NewRandom()
	mostSignificantBits, leastSignificantBits := ConstructLeastAndMostSignificantBits(id)

	reconstructedId := ReconstructUUID(mostSignificantBits, leastSignificantBits)

	require.Equal(t, id.String(), reconstructedId.String())
}

func Test_ReconstructId(t *testing.T) {
	id := uuid.MustParse("0c18cca5-1aa1-448f-9598-bc7af6b1ca72")

	mostSignificantBits := int64(871671537384637583)
	leastSignificantBits := int64(-7667171129287390606)
	reconstructedId := ReconstructUUID(mostSignificantBits, leastSignificantBits)

	require.Equal(t, id.String(), reconstructedId.String())
}

func Test_ConstructUUIDBits(t *testing.T) {
	id := uuid.MustParse("0c18cca5-1aa1-448f-9598-bc7af6b1ca72")
	mostSignificantBits, leastSignificantBits := ConstructLeastAndMostSignificantBits(id)

	require.Equal(t, int64(871671537384637583), mostSignificantBits)
	require.Equal(t, int64(-7667171129287390606), leastSignificantBits)
}
