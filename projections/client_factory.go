package projections

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/protos/projections"
)

type ClientFactory interface {
	CreateClient(grpcClient connection.GrpcClient, projectionsClient projections.ProjectionsClient) Client
}

type ClientFactoryImpl struct{}

func (clientFactory ClientFactoryImpl) CreateClient(
	grpcClient connection.GrpcClient,
	projectionsClient projections.ProjectionsClient) Client {
	return newClientImpl(grpcClient, projectionsClient)
}
