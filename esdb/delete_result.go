package esdb

// DeleteResult is returned on a successful stream deletion request.
type DeleteResult struct {
	// Transaction log position of the stream deletion.
	Position Position
}
