package persistent

//go:generate mockgen -source=grpc_subscription_client_factory.go -destination=grpc_subscription_client_factory_mock.go -package=persistent

import (
	persistentProto "github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
)

type grpcSubscriptionClientFactory interface {
	Create(*grpc.ClientConn) persistentProto.PersistentSubscriptionsClient
}

type grpcSubscriptionClientFactoryImpl struct{}

func (factory grpcSubscriptionClientFactoryImpl) Create(
	conn *grpc.ClientConn) persistentProto.PersistentSubscriptionsClient {
	return persistentProto.NewPersistentSubscriptionsClient(conn)
}
