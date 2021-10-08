package protobuf_uuid

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/bits"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

func GetUUID(protoUUID *shared.UUID) uuid.UUID {
	if structured := protoUUID.GetStructured(); structured != nil {
		return ReconstructUUID(structured.MostSignificantBits, structured.LeastSignificantBits)
	}

	return uuid.MustParse(protoUUID.GetString_())
}

func ReconstructUUID(mostSignificantBits, leasSignificantBits int64) uuid.UUID {
	lowBytes := reconstructUUIDHalfBytes(mostSignificantBits)
	topBytes := reconstructUUIDHalfBytes(leasSignificantBits)

	resultBytes := append(lowBytes, topBytes...)
	resultId, err := uuid.FromBytes(resultBytes)
	if err != nil {
		log.Fatalln(err)
	}

	return resultId
}

func reconstructUUIDHalfBytes(halfBits int64) []byte {
	byteBuffer := new(bytes.Buffer)

	reversedBits := bits.ReverseBytes64(uint64(halfBits))
	err := binary.Write(byteBuffer, binary.LittleEndian, reversedBits)
	if err != nil {
		log.Fatalln(err)
	}

	return byteBuffer.Bytes()
}

func ConstructLeastAndMostSignificantBits(id uuid.UUID) (mostSignificantBits, leastSignificantBits int64) {
	idBytes, _ := id.MarshalBinary()

	byteBuffer := bytes.NewBuffer(idBytes[:8])
	err := binary.Read(byteBuffer, binary.LittleEndian, &mostSignificantBits)
	if err != nil {
		log.Fatalln(err)
	}
	mostSignificantBits = int64(bits.ReverseBytes64(uint64(mostSignificantBits)))

	byteBuffer = bytes.NewBuffer(idBytes[8:])
	err = binary.Read(byteBuffer, binary.LittleEndian, &leastSignificantBits)
	if err != nil {
		log.Fatalln(err)
	}
	leastSignificantBits = int64(bits.ReverseBytes64(uint64(leastSignificantBits)))
	return
}
