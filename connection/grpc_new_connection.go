package connection

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
)

func newGetConnectionMsg() getConnection {
	return getConnection{
		channel: make(chan connectionHandle),
	}
}

const UUIDGeneratingError errors.ErrorCode = "UUIDGeneratingError"

func (msg getConnection) handle(state *connectionState) {
	// Means we need to create a grpc connection.
	if state.correlation == uuid.Nil {
		conn, err := discoverNode(state.config)
		if err != nil {
			state.lastError = err
			resp := newErroredConnectionHandle(err)
			msg.channel <- resp
			return
		}

		id, stdErr := uuid.NewRandom()
		if stdErr != nil {
			state.lastError = errors.NewErrorCodeMsg(UUIDGeneratingError,
				fmt.Sprintf("error when trying to generate a random UUID: %v", err))
			return
		}

		state.correlation = id
		state.connection = conn

		resp := newConnectionHandle(id, conn)
		msg.channel <- resp
	} else {
		handle := connectionHandle{
			id:         state.correlation,
			connection: state.connection,
			err:        nil,
		}

		msg.channel <- handle
	}
}
