package event_streams

type StreamAcl struct {
	ReadRoles      []string `json:"$r"`
	WriteRoles     []string `json:"$w"`
	DeleteRoles    []string `json:"$d"`
	MetaReadRoles  []string `json:"$mr"`
	MetaWriteRoles []string `json:"$mw"`
}
