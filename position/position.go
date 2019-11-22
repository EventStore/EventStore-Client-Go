package position

// Position ...
type Position struct {
	Commit  int64
	Prepare int64
}

// StartPosition ...
var StartPosition Position = Position{Commit: 0, Prepare: 0}

// EndPosition ...
var EndPosition Position = Position{Commit: -1, Prepare: -1}
