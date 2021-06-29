package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/gofrs/uuid"
)

type Nack_Action int32

const (
	Nack_Unknown Nack_Action = 0
	Nack_Park    Nack_Action = 1
	Nack_Retry   Nack_Action = 2
	Nack_Skip    Nack_Action = 3
	Nack_Stop    Nack_Action = 4
)

type SyncReadConnection interface {
	Read() (*messages.RecordedEvent, error) // this call must block
	Ack(messageIds ...uuid.UUID) error      // max 2000 messages can be acknowledged
	Nack(reason string, action Nack_Action, messageIds ...uuid.UUID) error
}
