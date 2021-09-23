package projections

import (
	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"google.golang.org/grpc"
)

type grpcProjectionsClientFactory interface {
	Create(connection *grpc.ClientConn) projections.ProjectionsClient
}

type grpcProjectionsClientFactoryImpl struct{}

func (factory grpcProjectionsClientFactoryImpl) Create(
	connection *grpc.ClientConn) projections.ProjectionsClient {
	return projections.NewProjectionsClient(connection)
}
