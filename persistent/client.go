package persistent

import "github.com/EventStore/EventStore-Client-Go/protos/persistent"

// ProtoClient is a proxy interface used to mock actual protobuf client in unit tests
type ProtoClient interface {
	Send(req *persistent.ReadReq) error
	Recv() (*persistent.ReadResp, error)
}
