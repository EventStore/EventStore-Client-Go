package connection

import (
	"log"

	"github.com/google/uuid"
)

type reconnect struct {
	correlation uuid.UUID
	endpoint    *EndPoint
}

func (msg reconnect) handle(state *connectionState) {
	if msg.correlation == state.correlation {
		if msg.endpoint == nil {
			// Means that in the next iteration cycle, the discovery process will start.
			state.correlation = uuid.Nil
			log.Printf("[info] Starting a new discovery process")
			return
		}

		log.Printf("[info] Connecting to leader node %s ...", msg.endpoint.String())
		conn, err := createGrpcConnection(state.config, msg.endpoint.String())
		if err != nil {
			log.Printf("[error] exception when connecting to suggested node %s", msg.endpoint.String())
			state.correlation = uuid.Nil
			return
		}

		id, err := uuid.NewRandom()
		if err != nil {
			log.Printf("[error] exception when generating a correlation id after reconnected to %s : %v", msg.endpoint.String(), err)
			state.correlation = uuid.Nil
			return
		}

		state.correlation = id
		state.connection = conn

		log.Printf("[info] Successfully connected to leader node %s", msg.endpoint.String())
	}
}
