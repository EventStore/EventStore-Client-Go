package projections

//go:generate mockgen -source=client_factory.go -destination=client_factory_mock.go -package=projections

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
)

type ClientFactory interface {
	CreateClient(grpcClient connection.GrpcClient) Client
}

type ClientFactoryImpl struct{}

func (clientFactory ClientFactoryImpl) CreateClient(
	grpcClient connection.GrpcClient) Client {
	return newClientImpl(grpcClient, grpcProjectionsClientFactoryImpl{})
}
