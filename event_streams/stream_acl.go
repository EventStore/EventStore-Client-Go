package event_streams

// StreamAcl represents an access control list for a stream.
// It is set through stream's metadata with Client.SetStreamMetadata.
// User must have a write access to a stream's metadata stream in order
// to be able to set access control list.
// Read more about stream ACL at https://developers.eventstore.com/server/v21.6/security/acl.html#stream-acl.
type StreamAcl struct {
	// ReadRoles is a list of users which can read from a stream.
	// If ReadRoles is empty that means that any user can read from a stream.
	ReadRoles []string `json:"$r"`
	// WriteRoles is a list of users which can write events to a stream.
	// If WriteRoles is empty that means that any user can write events to a stream.
	WriteRoles []string `json:"$w"`
	// DeleteRoles is a list of users  which can perform soft and hard delete of a stream.
	// If DeleteRoles is empty that means that any user can perform soft and hard delete of a stream.
	DeleteRoles []string `json:"$d"`
	// MetaReadRoles is a list of users which can read stream's metadata.
	// If MetaReadRoles is empty that means that any user can read stream's metadata.
	MetaReadRoles []string `json:"$mr"`
	// MetaWriteRoles is a list of users which can write stream's metadata.
	// If MetaWriteRoles is empty that means that any user can write stream's metadata.
	MetaWriteRoles []string `json:"$mw"`
}
