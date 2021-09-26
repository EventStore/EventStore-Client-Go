package persistent

//go:generate mockgen -source=sync_read_connection.go -destination=sync_read_connection_mock.go -package=persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/errors"
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
	ReadOne() (ReadResponseEvent, errors.Error) // this call must block
	Ack(msgs ...ReadResponseEvent) errors.Error // max 2000 messages can be acknowledged
	Nack(reason string, action Nack_Action, msgs ...ReadResponseEvent) error
	Close() error
}
