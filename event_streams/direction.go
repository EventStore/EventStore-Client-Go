package event_streams

// ReadDirection specifies a direction in which a stream will be read.
// Direction can be either forward or backward.
type ReadDirection string

const (
	ReadDirectionForward  ReadDirection = "ReadDirectionForward"
	ReadDirectionBackward ReadDirection = "ReadDirectionBackward"
)
