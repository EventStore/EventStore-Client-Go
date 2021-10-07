package protobuf_uuid

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/bits"

	"github.com/pivonroll/EventStore-Client-Go/protos/shared"

	"github.com/gofrs/uuid"
)

func GetUUID(protoUUID *shared.UUID) uuid.UUID {
	if structured := protoUUID.GetStructured(); structured != nil {
		return ReconstructUUID(structured.MostSignificantBits, structured.LeastSignificantBits)
	}

	return uuid.FromStringOrNil(protoUUID.GetString_())
}

func ReconstructUUID(mostSignificant, leasSignificant int64) uuid.UUID {
	lowBuffer := new(bytes.Buffer)
	highBuffer := new(bytes.Buffer)

	mostSignificantRes := bits.ReverseBytes64(uint64(mostSignificant))
	err := binary.Write(lowBuffer, binary.LittleEndian, mostSignificantRes)
	if err != nil {
		log.Fatalln(err)
	}

	leasSignificantRes := bits.ReverseBytes64(uint64(leasSignificant))
	err = binary.Write(highBuffer, binary.LittleEndian, leasSignificantRes)
	if err != nil {
		log.Fatalln(err)
	}

	lowBytes := lowBuffer.Bytes()
	topBytes := highBuffer.Bytes()

	resultBytes := append(lowBytes, topBytes...)
	resultId, err := uuid.FromBytes(resultBytes)
	if err != nil {
		log.Fatalln(err)
	}

	return resultId
}
