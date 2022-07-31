package esdb

// ServerVersion Represents the version of an EventStoreDB node.
type ServerVersion struct {
	Major int
	Minor int
	Patch int
}

// GetServerVersion Returns the version of the EventStoreDB node to which the client is currently connected.
func (client *Client) GetServerVersion() (*ServerVersion, error) {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	return handle.GetServerVersion();
}