package connection

import (
	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"google.golang.org/grpc"
)

type connectionHandle struct {
	id         uuid.UUID
	connection *grpc.ClientConn
	err        errors.Error
}

func (handle connectionHandle) Id() uuid.UUID {
	return handle.id
}

func (handle connectionHandle) Connection() *grpc.ClientConn {
	return handle.connection
}

func newErroredConnectionHandle(err errors.Error) connectionHandle {
	return connectionHandle{
		id:         uuid.Nil,
		connection: nil,
		err:        err,
	}
}

func newConnectionHandle(id uuid.UUID, connection *grpc.ClientConn) connectionHandle {
	return connectionHandle{
		id:         id,
		connection: connection,
		err:        nil,
	}
}
