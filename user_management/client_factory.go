package user_management

import (
	"context"
	"sync"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/protos/users"
	"google.golang.org/grpc"
)

type ClientFactory interface {
	Create(grpcClient connection.GrpcClient) Client
}

type ClientFactoryImpl struct{}

func (factory ClientFactoryImpl) Create(grpcClient connection.GrpcClient) Client {
	return newClientImpl(grpcClient, grpcUserClientFactoryImpl{}, detailsReaderFactoryImpl{})
}

type grpcUserClientFactory interface {
	Create(cc grpc.ClientConnInterface) users.UsersClient
}

type grpcUserClientFactoryImpl struct{}

func (factory grpcUserClientFactoryImpl) Create(cc grpc.ClientConnInterface) users.UsersClient {
	return users.NewUsersClient(cc)
}

type DetailsReaderFactory interface {
	Create(protoStreamReader users.Users_DetailsClient,
		cancelFunc context.CancelFunc) DetailsReader
}

type detailsReaderFactoryImpl struct{}

func (factory detailsReaderFactoryImpl) Create(
	protoStreamReader users.Users_DetailsClient,
	cancelFunc context.CancelFunc) DetailsReader {
	return &DetailsReaderImpl{
		protoStreamReader:      protoStreamReader,
		detailsResponseAdapter: detailsResponseAdapterImpl{},
		once:                   sync.Once{},
		cancelFunc:             cancelFunc,
	}
}
