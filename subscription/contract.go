package subscription

import "google.golang.org/grpc"

type Subscription interface {
	OnConnectionUpdate(conn *grpc.ClientConn)
	Start() error
	Stop() error
}
