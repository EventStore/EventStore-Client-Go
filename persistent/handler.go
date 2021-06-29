package persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/messages"
)

// this interface is used fo mocking handlers in unit tests
type handlerTest interface {
	handler(context.Context, messages.RecordedEvent) error
}
