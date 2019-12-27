package esgrpc

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"log"
	"strconv"
	"time"

	api "github.com/eventstore/EventStore-Client-Go/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	auth := b.username + ":" + b.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{
		"Authorization": "Basic " + enc,
	}, nil
}

func (basicAuth) RequireTransportSecurity() bool {
	return false
}

// Configuration ...
type Configuration struct {
	Address                     string
	Username                    string
	Password                    string
	SkipCertificateVerification bool
	Connected                   bool
}

// NewConfiguration ...
func NewConfiguration() *Configuration {
	return &Configuration{
		Address:                     "localhost:2113",
		Username:                    "admin",
		Password:                    "changeit",
		SkipCertificateVerification: false,
	}
}

// Client ...
type Client struct {
	Config     *Configuration
	Connection *grpc.ClientConn
}

// NewClient ...
func NewClient(configuration *Configuration) (*Client, error) {
	return &Client{
		Config: configuration,
	}, nil
}

// Connect ...
func (client *Client) Connect() error {
	config := &tls.Config{
		InsecureSkipVerify: client.Config.SkipCertificateVerification,
	}
	conn, err := grpc.Dial(client.Config.Address, grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithPerRPCCredentials(basicAuth{
		username: client.Config.Username,
		password: client.Config.Password,
	}))
	if err != nil {
		log.Printf("Failed to initialize connection to %+v. Details: %+v", client.Config, err)
		return err
	}
	client.Connection = conn
	return nil
}

// Close ...
func (client *Client) Close() error {
	return client.Connection.Close()
}

// AppendToStream ...
func (client *Client) AppendToStream(streamID string, streamRevision StreamRevision, messages []Message) (*api.AppendResp, error) {
	streamsClient := api.NewStreamsClient(client.Connection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	appendOperation, err := streamsClient.Append(ctx)
	if err != nil {
		log.Printf("Could not construct append operation: %v", err)
		return nil, err
	}

	header := &api.AppendReq{
		Content: &api.AppendReq_Options_{
			Options: &api.AppendReq_Options{
				StreamName: streamID,
				ExpectedStreamRevision: &api.AppendReq_Options_Any{
					Any: &api.AppendReq_Empty{},
				},
			},
		},
	}

	switch streamRevision {
	case StreamRevisionAny:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &api.AppendReq_Empty{},
		}
	case StreamRevisionNoStream:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &api.AppendReq_Empty{},
		}
	case StreamRevisionStreamExists:
		header.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &api.AppendReq_Empty{},
		}
	}

	if err := appendOperation.Send(header); err != nil {
		log.Printf("Could not send append request header %+v", err)
		return nil, err
	}

	for _, message := range messages {
		metadata := make(map[string]string)
		for key, value := range message.Metadata {
			metadata[key] = value
		}
		metadata["type"] = message.EventType
		metadata["is-json"] = strconv.FormatBool(message.IsJSON)
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: &api.AppendReq_ProposedMessage{
					Id: &api.UUID{
						Value: &api.UUID_String_{
							String_: message.EventID.String(),
						},
					},
					Data:           message.Data,
					CustomMetadata: []byte("{}"),
					Metadata:       metadata,
				},
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			log.Printf("Could not send append request: %v", err)
			return nil, err
		}
	}

	return appendOperation.CloseAndRecv()
}
