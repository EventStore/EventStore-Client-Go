package event_streams

type StreamAcl struct {
	ReadRoles      []string
	WriteRoles     []string
	DeleteRoles    []string
	MetaReadRoles  []string
	MetaWriteRoles []string
}
