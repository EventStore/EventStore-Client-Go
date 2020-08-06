package client

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"

	direction "github.com/EventStore/EventStore-Client-Go/direction"
	errors "github.com/EventStore/EventStore-Client-Go/errors"
	protoutils "github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	messages "github.com/EventStore/EventStore-Client-Go/messages"
	position "github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(tx context.Context, in ...string) (map[string]string, error) {
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
		return fmt.Errorf("Failed to initialize connection to %+v. Reason: %v", client.Config, err)
	}
	client.Connection = conn
	return nil
}

// Close ...
func (client *Client) Close() error {
	return client.Connection.Close()
}

// AppendToStream ...
func (client *Client) AppendToStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision, events []messages.ProposedEvent) (*WriteResult, error) {
	streamsClient := api.NewStreamsClient(client.Connection)

	appendOperation, err := streamsClient.Append(context)
	if err != nil {
		return nil, fmt.Errorf("Could not construct append operation. Reason: %v", err)
	}

	header := protoutils.ToAppendHeaderFromStreamIDAndStreamRevision(streamID, streamRevision)

	if err := appendOperation.Send(header); err != nil {
		return nil, fmt.Errorf("Could not send append request header. Reason: %v", err)
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: protoutils.ToProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			return nil, fmt.Errorf("Could not send append request. Reason: %v", err)
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		status, _ := status.FromError(err)
		if status.Code() == codes.FailedPrecondition { //Precondition -> ErrWrongExpectedStremRevision
			return nil, fmt.Errorf("%w", errors.ErrWrongExpectedStreamRevision)
		}
		if status.Code() == codes.PermissionDenied { //PermissionDenied -> ErrPemissionDenied
			return nil, fmt.Errorf("%w", errors.ErrPermissionDenied)
		}
		if status.Code() == codes.Unauthenticated { //PermissionDenied -> ErrUnauthenticated
			return nil, fmt.Errorf("%w", errors.ErrUnauthenticated)
		}
		return nil, err
	}
	return WriteResultFromAppendResp(response)
}

// SoftDeleteStream ...
func (client *Client) SoftDeleteStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision) (*DeleteResult, error) {
	streamsClient := api.NewStreamsClient(client.Connection)

	deleteReq := &api.DeleteReq{
		Options: &api.DeleteReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamID),
			},
		},
	}
	switch streamRevision {
	case stream_revision.StreamRevisionAny:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Any{
			Any: &shared.Empty{},
		}
	case stream_revision.StreamRevisionNoStream:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case stream_revision.StreamRevisionStreamExists:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	default:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Revision{
			Revision: uint64(streamRevision),
		}
	}
	deleteResponse, err := streamsClient.Delete(context, deleteReq)

	if err != nil {
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{
		CommitPosition:  deleteResponse.GetPosition().CommitPosition,
		PreparePosition: deleteResponse.GetPosition().PreparePosition,
	}, nil
}

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(context context.Context, direction direction.Direction, streamID string, streamRevision uint64, count uint64, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: protoutils.ToReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  protoutils.ToReadStreamOptionsFromStreamAndStreamRevision(streamID, streamRevision),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	return readInternal(context, client.Connection, readReq, count)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(context context.Context, direction direction.Direction, position position.Position, count uint64, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: protoutils.ToReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  protoutils.ToAllReadOptionsFromPosition(position),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	return readInternal(context, client.Connection, readReq, count)
}

func readInternal(context context.Context, connection *grpc.ClientConn, readRequest *api.ReadReq, limit uint64) ([]messages.RecordedEvent, error) {
	streamsClient := api.NewStreamsClient(connection)

	result, err := streamsClient.Read(context, readRequest)

	if err != nil {
		return []messages.RecordedEvent{}, fmt.Errorf("Failed to construct read client. Reason: %v", err)
	}

	events := []messages.RecordedEvent{}
	for {
		readResult, err := result.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
		}
		switch readResult.Content.(type) {
		case *api.ReadResp_Checkpoint_:
			{

			}
		case *api.ReadResp_Confirmation:
			{

			}
		case *api.ReadResp_Event:
			{
				event := readResult.GetEvent()
				recordedEvent := event.GetEvent()
				streamIdentifier := recordedEvent.GetStreamIdentifier()

				events = append(events, messages.RecordedEvent{
					EventID:        protoutils.EventIDFromProto(recordedEvent),
					EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
					ContentType:    protoutils.GetContentTypeFromProto(recordedEvent),
					StreamID:       string(streamIdentifier.StreamName),
					EventNumber:    recordedEvent.GetStreamRevision(),
					CreatedDate:    protoutils.CreatedFromProto(recordedEvent),
					Position:       protoutils.PositionFromProto(recordedEvent),
					Data:           recordedEvent.GetData(),
					SystemMetadata: recordedEvent.GetMetadata(),
					UserMetadata:   recordedEvent.GetCustomMetadata(),
				})
				if uint64(len(events)) >= limit {
					break
				}
			}
		case *api.ReadResp_StreamNotFound_:
			{

			}
		}
	}
	return events, nil
}
