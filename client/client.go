package client

import (
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/pivonroll/EventStore-Client-Go/projections"
	"github.com/pivonroll/EventStore-Client-Go/user_management"
)

type Configuration = connection.Configuration

func ParseConnectionString(str string) (*connection.Configuration, error) {
	return connection.ParseConnectionString(str)
}

// Client ...
type Client struct {
	grpcClient                connection.GrpcClient
	Config                    *connection.Configuration
	persistentClientFactory   persistent.ClientFactory
	projectionClientFactory   projections.ClientFactory
	eventStreamsClientFactory event_streams.ClientFactory
	userManagementFactory     user_management.ClientFactory
}

// NewClient ...
func NewClient(configuration *connection.Configuration) (*Client, error) {
	grpcClient := connection.NewGrpcClient(*configuration)
	return &Client{
		grpcClient:                grpcClient,
		Config:                    configuration,
		persistentClientFactory:   persistent.ClientFactoryImpl{},
		projectionClientFactory:   projections.ClientFactoryImpl{},
		eventStreamsClientFactory: event_streams.ClientFactoryImpl{},
		userManagementFactory:     user_management.ClientFactoryImpl{},
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.Close()
	return nil
}

func (client *Client) Projections() projections.Client {
	return client.projectionClientFactory.CreateClient(client.grpcClient)
}

func (client *Client) UserManagement() user_management.Client {
	return client.userManagementFactory.Create(client.grpcClient)
}

func (client *Client) EventStreams() event_streams.Client {
	return client.eventStreamsClientFactory.CreateClient(client.grpcClient)
}

func (client *Client) PersistentSubscriptions() persistent.Client {
	return client.persistentClientFactory.CreateClient(client.grpcClient)
}
