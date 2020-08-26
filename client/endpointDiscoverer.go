package client

//MemberInfo represents the members in a cluster which is retrieved as part of the gossip request and lives inside of the members in the response
type MemberInfo struct {
	State            NodeState `json:"state"`
	IsAlive          bool      `json:"isAlive"`
	ExternalTCPIP    string    `json:"externalTcpIp"`
	ExternalTCPPort  int       `json:"externalTcpPort"`
	HttpEndPointIP   string    `json:"HttpEndPointIp"`
	HttpEndPointPort int       `json:"HttpEndPointPort"`
}

type NodePreference string

const (
	NodePreference_Leader          NodePreference = "Leader"
	NodePreference_Follower        NodePreference = "Follower"
	NodePreference_ReadOnlyReplica NodePreference = "ReadOnlyReplica"
	NodePreference_Random          NodePreference = "Random"
)

func (nodePreference NodePreference) String() string {
	return string(nodePreference)
}

type NodeState string

const (
	Leader             NodeState = "Leader"
	Follower           NodeState = "Follower"
	Manager            NodeState = "Manager"
	Shutdown           NodeState = "Shutdown"
	Unknown            NodeState = "Unknown"
	Initializing       NodeState = "Initializing"
	CatchingUp         NodeState = "CatchingUp"
	ResigningLeader    NodeState = "ResigningLeader"
	ShuttingDown       NodeState = "ShuttingDown"
	PreLeader          NodeState = "PreLeader"
	PreReplica         NodeState = "PreReplica"
	PreReadOnlyReplica NodeState = "PreReadOnlyReplica"
	Clone              NodeState = "Clone"
	DiscoverLeader     NodeState = "DiscoverLeader"
	ReadOnlyLeaderless NodeState = "ReadOnlyLeaderless"
	ReadOnlyReplica    NodeState = "ReadOnlyReplica"
)

func (nodeState NodeState) String() string {
	return string(nodeState)
}

//EndpointDiscoverer func that is used to discover an endpoint given the gossip seeds
type EndpointDiscoverer interface {
	Discover() (*MemberInfo, error)
}
