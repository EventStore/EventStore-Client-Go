package position

// Position ...
type Position struct {
	Commit  uint64
	Prepare uint64
}

// EmptyPosition ...
var EmptyPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}

// StartPosition ...
var StartPosition Position = Position{Commit: 0, Prepare: 0}

// EndPosition ...
var EndPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}
