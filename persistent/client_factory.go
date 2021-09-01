package persistent

//go:generate mockgen -source=client_factory.go -destination=client_factory_mock.go -package=persistent

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
)

type ClientFactory interface {
	CreateClient(grpcClient connection.GrpcClient, client persistentProto.PersistentSubscriptionsClient) Client
}

type ClientFactoryImpl struct{}

func (clientFactory ClientFactoryImpl) CreateClient(grpcClient connection.GrpcClient, client persistentProto.PersistentSubscriptionsClient) Client {
	return newClientImpl(grpcClient, client)
}
