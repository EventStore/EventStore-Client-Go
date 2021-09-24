package projections

//go:generate mockgen -source=grpc_projections_client_factory.go -destination=grpc_projections_client_factory_mock.go -package=projections

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
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
