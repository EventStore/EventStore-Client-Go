package connection

type close struct {
	channel chan bool
}

func (msg close) handle(state *connectionState) {
	state.closed = true
	if state.connection != nil {
		defer func() {
			state.connection.Close()
			state.connection = nil
		}()
	}

	msg.channel <- true
}
