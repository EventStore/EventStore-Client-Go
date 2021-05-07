package client

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/EventStore/EventStore-Client-Go/subscription"
)

// Client ...
type Client struct {
	Config        *Configuration
	Connection    *grpc.ClientConn
	streamsClient api.StreamsClient
}

// NewClient ...
func NewClient(configuration *Configuration) (*Client, error) {
	return &Client{
		Config: configuration,
	}, nil
}

// Connect ...
func (client *Client) Connect() error {
	if len(client.Config.GossipSeeds) > 0 {
		discoverer := NewGossipEndpointDiscoverer()

		seeds := make([]*url.URL, 0)
		for _, seed := range client.Config.GossipSeeds {
			seedUrl, err := url.Parse(seed)
			if err != nil {
				return fmt.Errorf("The gossip seed (%s) is invalid and is required to be in the format of {host}:{port}, details: %s", seed, err.Error())
			}
			seeds = append(seeds, seedUrl)
		}
		discoverer.GossipSeeds = seeds
		discoverer.MaxDiscoverAttempts = client.Config.MaxDiscoverAttempts
		discoverer.NodePreference = client.Config.NodePreference
		discoverer.SkipCertificateValidation = client.Config.SkipCertificateVerification

		preferedNode, err := discoverer.Discover()
		if err != nil {
			return fmt.Errorf("Failed to connect due to discovery failure, details: %s", err.Error())
		}
		preferedNodeAddress := fmt.Sprintf("%s:%d", preferedNode.HttpEndPointIP, preferedNode.HttpEndPointPort)
		if err != nil {
			return fmt.Errorf("Failed to transform prefered node (%+v) into address returned from discovery: details: %+v", preferedNode, err)
		}
		client.Config.Address = preferedNodeAddress
	}

	var opts []grpc.DialOption
	if client.Config.DisableTLS {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts,
			grpc.WithTransportCredentials(credentials.NewTLS(
				&tls.Config{
					InsecureSkipVerify: client.Config.SkipCertificateVerification,
					RootCAs:            client.Config.RootCAs,
				})))
	}
	opts = append(opts, grpc.WithPerRPCCredentials(basicAuth{
		username: client.Config.Username,
		password: client.Config.Password,
	}))

	if client.Config.KeepAliveInterval >= 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                client.Config.KeepAliveInterval,
			Timeout:             client.Config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	}

	conn, err := grpc.Dial(client.Config.Address, opts...)
	if err != nil {
		return fmt.Errorf("Failed to initialize connection to %+v. Reason: %v", client.Config, err)
	}
	client.Connection = conn
	client.streamsClient = api.NewStreamsClient(client.Connection)

	return nil
}

// Close ...
func (client *Client) Close() error {
	return client.Connection.Close()
}

// AppendToStream ...
func (client *Client) AppendToStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision, events []messages.ProposedEvent) (*WriteResult, error) {
	appendOperation, err := client.streamsClient.Append(context)
	if err != nil {
		return nil, fmt.Errorf("Could not construct append operation. Reason: %v", err)
	}

	header := protoutils.ToAppendHeader(streamID, streamRevision)

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
			return nil, fmt.Errorf("%w, reason: %s", errors.ErrWrongExpectedStreamRevision, err.Error())
		}
		if status.Code() == codes.PermissionDenied { //PermissionDenied -> ErrPemissionDenied
			return nil, fmt.Errorf("%w", errors.ErrPermissionDenied)
		}
		if status.Code() == codes.Unauthenticated { //PermissionDenied -> ErrUnauthenticated
			return nil, fmt.Errorf("%w", errors.ErrUnauthenticated)
		}
		return nil, err
	}

	result := response.GetResult()
	switch result.(type) {
	case *api.AppendResp_Success_:
		{
			success := result.(*api.AppendResp_Success_)
			var streamRevision uint64
			if _, ok := success.Success.GetCurrentRevisionOption().(*api.AppendResp_Success_NoStream); ok {
				streamRevision = 1
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			var commitPosition uint64
			var preparePosition uint64
			if position, ok := success.Success.GetPositionOption().(*api.AppendResp_Success_Position); ok {
				commitPosition = position.Position.CommitPosition
				preparePosition = position.Position.PreparePosition
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			return &WriteResult{
				CommitPosition:      commitPosition,
				PreparePosition:     preparePosition,
				NextExpectedVersion: streamRevision,
			}, nil
		}
	case *api.AppendResp_WrongExpectedVersion_:
		{
			return nil, errors.ErrWrongExpectedStreamRevision
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

// DeleteStream ...
func (client *Client) DeleteStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision) (*DeleteResult, error) {
	deleteRequest := protoutils.ToDeleteRequest(streamID, streamRevision)
	deleteResponse, err := client.streamsClient.Delete(context, deleteRequest)

	if err != nil {
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.DeletePositionFromProto(deleteResponse)}, nil
}

// Tombstone ...
func (client *Client) TombstoneStream(context context.Context, streamID string, streamRevision stream_revision.StreamRevision) (*DeleteResult, error) {
	tombstoneRequest := protoutils.ToTombstoneRequest(streamID, streamRevision)
	tombstoneResponse, err := client.streamsClient.Tombstone(context, tombstoneRequest)

	if err != nil {
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.TombstonePositionFromProto(tombstoneResponse)}, nil
}

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(context context.Context, direction direction.Direction, streamID string, from uint64, count uint64, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readRequest := protoutils.ToReadStreamRequest(streamID, direction, from, count, resolveLinks)
	return readInternal(context, client.streamsClient, readRequest, count)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(context context.Context, direction direction.Direction, from position.Position, count uint64, resolveLinks bool) ([]messages.RecordedEvent, error) {
	readRequest := protoutils.ToReadAllRequest(direction, from, count, resolveLinks)
	return readInternal(context, client.streamsClient, readRequest, count)
}

// SubscribeToStream ...
func (client *Client) SubscribeToStream(context context.Context, streamID string, from uint64, resolveLinks bool, eventAppeared chan<- interface{}, subscriptionDropped chan<- string) (*subscription.Subscription, error) {
	subscriptionRequest, err := protoutils.ToStreamSubscriptionRequest(streamID, from, resolveLinks, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readClient, err := client.streamsClient.Read(context, subscriptionRequest)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId, eventAppeared, subscriptionDropped), nil
		}
	case *api.ReadResp_StreamNotFound_:
		{
			return nil, fmt.Errorf("Failed to initiate subscription because the stream (%s) was not found.", streamID)
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// SubscribeToAll ...
func (client *Client) SubscribeToAll(context context.Context, from position.Position, resolveLinks bool, eventAppeared chan<- interface{}, subscriptionDropped chan<- string) (*subscription.Subscription, error) {
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(from, resolveLinks, nil)
	readClient, err := client.streamsClient.Read(context, subscriptionRequest)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId, eventAppeared, subscriptionDropped), nil
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// SubscribeToAllFiltered ...
func (client *Client) SubscribeToAllFiltered(context context.Context, from position.Position, resolveLinks bool, filterOptions filtering.SubscriptionFilterOptions, eventAppeared chan<- interface{}, subscriptionDropped chan<- string) (*subscription.Subscription, error) {
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(from, resolveLinks, &filterOptions)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readClient, err := client.streamsClient.Read(context, subscriptionRequest)
	if err != nil {
		return nil, fmt.Errorf("Failed to initiate subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		return nil, fmt.Errorf("Failed to read from subscription. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId, eventAppeared, subscriptionDropped), nil
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

func readInternal(context context.Context, streamsClient api.StreamsClient, readRequestuest *api.ReadReq, limit uint64) ([]messages.RecordedEvent, error) {
	result, err := streamsClient.Read(context, readRequestuest)

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
		case *api.ReadResp_Event:
			{
				event := readResult.GetEvent()
				recordedEvent := protoutils.RecordedEventFromProto(event)
				events = append(events, recordedEvent)
				if uint64(len(events)) >= limit {
					break
				}
			}
		case *api.ReadResp_StreamNotFound_:
			{
				return nil, errors.ErrStreamNotFound
			}
		}
	}
	return events, nil
}

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
