package operations

//go:generate mockgen -source=grpc_operations_client_factory.go -destination=grpc_operations_client_factory_mock.go -package=operations

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/operations"
	"google.golang.org/grpc"
)

type grpcOperationsClientFactory interface {
	Create(*grpc.ClientConn) operations.OperationsClient
}

type grpcOperationsClientFactoryImpl struct{}

func (factory grpcOperationsClientFactoryImpl) Create(
	conn *grpc.ClientConn) operations.OperationsClient {
	return operations.NewOperationsClient(conn)
}
