package esdb

import (
	"context"
	"errors"
	"io"

	"github.com/EventStore/EventStore-Client-Go/v4/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/v4/protos/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type ProjectionClient struct {
	inner *Client
}

func NewProjectionClient(configuration *Configuration) (*ProjectionClient, error) {
	client, err := NewClient(configuration)

	if err != nil {
		return nil, err
	}

	return &ProjectionClient{
		inner: client,
	}, nil
}

func NewProjectionClientFromExistingClient(client *Client) *ProjectionClient {
	return &ProjectionClient{
		inner: client,
	}
}

func (client *ProjectionClient) Client() *Client {
	return client.inner
}

func (client *ProjectionClient) Close() error {
	return client.inner.Close()
}

func (client *ProjectionClient) Create(
	context context.Context,
	name string,
	query string,
	opts CreateProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.Create(ctx, &projections.CreateReq{
		Options: &projections.CreateReq_Options{
			Query: query,
			Mode: &projections.CreateReq_Options_Continuous_{
				Continuous: &projections.CreateReq_Options_Continuous{
					Name:                name,
					EmitEnabled:         opts.Emit,
					TrackEmittedStreams: opts.TrackEmittedStreams,
				},
			},
		},
	}, callOptions...)

	return err
}

func (client *ProjectionClient) Update(
	context context.Context,
	name string,
	query string,
	opts UpdateProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	options := &projections.UpdateReq_Options{
		Name:  name,
		Query: query,
	}

	if opts.Emit == nil {
		options.EmitOption = &projections.UpdateReq_Options_NoEmitOptions{}
	} else {
		options.EmitOption = &projections.UpdateReq_Options_EmitEnabled{
			EmitEnabled: *opts.Emit,
		}
	}

	_, err = projClient.Update(ctx, &projections.UpdateReq{
		Options: options,
	}, callOptions...)

	return err
}

func (client *ProjectionClient) Delete(
	context context.Context,
	name string,
	opts DeleteProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.Delete(ctx, &projections.DeleteReq{
		Options: &projections.DeleteReq_Options{
			Name:                   name,
			DeleteEmittedStreams:   opts.DeleteEmittedStreams,
			DeleteStateStream:      opts.DeleteStateStream,
			DeleteCheckpointStream: opts.DeleteCheckpointStream,
		},
	}, callOptions...)

	return err
}

func (client *ProjectionClient) Enable(
	context context.Context,
	name string,
	opts GenericProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.Enable(ctx, &projections.EnableReq{
		Options: &projections.EnableReq_Options{
			Name: name,
		},
	}, callOptions...)

	return err
}

func (client *ProjectionClient) Disable(
	context context.Context,
	name string,
	opts GenericProjectionOptions,
) error {
	return client.disable(context, name, true, opts)
}

func (client *ProjectionClient) Abort(
	context context.Context,
	name string,
	opts GenericProjectionOptions,
) error {
	return client.disable(context, name, false, opts)
}

func (client *ProjectionClient) disable(
	context context.Context,
	name string,
	writeCheckpoint bool,
	opts GenericProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.Disable(ctx, &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            name,
			WriteCheckpoint: writeCheckpoint,
		},
	}, callOptions...)

	return err
}

func (client *ProjectionClient) Reset(
	context context.Context,
	name string,
	opts ResetProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.Reset(ctx, &projections.ResetReq{
		Options: &projections.ResetReq_Options{
			Name:            name,
			WriteCheckpoint: opts.WriteCheckpoint,
		},
	}, callOptions...)

	return err
}

func (client *ProjectionClient) GetResult(
	context context.Context,
	name string,
	opts GetResultProjectionOptions,
) (*structpb.Value, error) {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	resp, err := projClient.Result(ctx, &projections.ResultReq{
		Options: &projections.ResultReq_Options{
			Name:      name,
			Partition: opts.Partition,
		},
	}, callOptions...)

	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

func (client *ProjectionClient) GetState(
	context context.Context,
	name string,
	opts GetStateProjectionOptions,
) (*structpb.Value, error) {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	resp, err := projClient.State(ctx, &projections.StateReq{
		Options: &projections.StateReq_Options{
			Name:      name,
			Partition: opts.Partition,
		},
	}, callOptions...)

	if err != nil {
		return nil, err
	}

	return resp.State, nil
}

func (client *ProjectionClient) RestartSubsystem(
	context context.Context,
	opts GenericProjectionOptions,
) error {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	_, err = projClient.RestartSubsystem(ctx, &shared.Empty{}, callOptions...)

	return err
}

type ProjectionStatus struct {
	CoreProcessingTime                 int64
	Version                            int64
	Epoch                              int64
	EffectiveName                      string
	WritesInProgress                   int32
	ReadsInProgress                    int32
	PartitionsCached                   int32
	Status                             string
	StateReason                        string
	Name                               string
	Mode                               string
	Position                           string
	Progress                           float32
	LastCheckpoint                     string
	EventsProcessedAfterRestart        int64
	CheckpointStatus                   string
	BufferedEvents                     int64
	WritePendingEventsBeforeCheckpoint int32
	WritePendingEventsAfterCheckpoint  int32
}

type projectionKind interface {
	setProjectionMode(opts *projections.StatisticsReq_Options)
}

type named struct {
	name string
}

func (name named) setProjectionMode(opts *projections.StatisticsReq_Options) {
	opts.Mode = &projections.StatisticsReq_Options_Name{
		Name: name.name,
	}
}

type projectionSelect int

const (
	projectionSelectContinuous projectionSelect = iota
	projectionSelectAll
)

func (projSelect projectionSelect) setProjectionMode(opts *projections.StatisticsReq_Options) {
	switch projSelect {
	case projectionSelectContinuous:
		opts.Mode = &projections.StatisticsReq_Options_Continuous{}
	case projectionSelectAll:
		opts.Mode = &projections.StatisticsReq_Options_All{}
	}
}

func (client *ProjectionClient) GetStatus(
	context context.Context,
	name string,
	opts GenericProjectionOptions,
) (*ProjectionStatus, error) {
	projs, err := client.listInternal(context, named{name: name}, opts)

	if err != nil {
		return nil, err
	}

	return &projs[0], nil
}

func (client *ProjectionClient) ListContinuous(
	context context.Context,
	opts GenericProjectionOptions,
) ([]ProjectionStatus, error) {
	return client.listInternal(context, projectionSelectContinuous, opts)
}

func (client *ProjectionClient) ListAll(
	context context.Context,
	opts GenericProjectionOptions,
) ([]ProjectionStatus, error) {
	return client.listInternal(context, projectionSelectAll, opts)
}

func (client *ProjectionClient) listInternal(
	context context.Context,
	kind projectionKind,
	opts GenericProjectionOptions,
) ([]ProjectionStatus, error) {
	opts.setDefaults()
	handle, err := client.inner.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	projClient := projections.NewProjectionsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.inner.config, &opts, callOptions, client.inner.grpcClient.perRPCCredentials)
	defer cancel()

	options := projections.StatisticsReq_Options{}
	kind.setProjectionMode(&options)
	stream, err := projClient.Statistics(ctx, &projections.StatisticsReq{
		Options: &options,
	}, callOptions...)

	var projs []ProjectionStatus

	for {
		item, err := stream.Recv()

		if err != nil {
			if !errors.Is(err, io.EOF) {
				err = client.inner.grpcClient.handleError(handle, trailers, err)
				return nil, err
			}

			return projs, nil
		}

		proj := ProjectionStatus{
			CoreProcessingTime:                 item.Details.CoreProcessingTime,
			Version:                            item.Details.Version,
			Epoch:                              item.Details.Epoch,
			EffectiveName:                      item.Details.EffectiveName,
			WritesInProgress:                   item.Details.WritesInProgress,
			ReadsInProgress:                    item.Details.ReadsInProgress,
			PartitionsCached:                   item.Details.PartitionsCached,
			Status:                             item.Details.Status,
			StateReason:                        item.Details.StateReason,
			Name:                               item.Details.Name,
			Mode:                               item.Details.Mode,
			Position:                           item.Details.Position,
			Progress:                           item.Details.Progress,
			LastCheckpoint:                     item.Details.LastCheckpoint,
			EventsProcessedAfterRestart:        item.Details.EventsProcessedAfterRestart,
			CheckpointStatus:                   item.Details.CheckpointStatus,
			BufferedEvents:                     item.Details.BufferedEvents,
			WritePendingEventsBeforeCheckpoint: item.Details.WritePendingEventsBeforeCheckpoint,
			WritePendingEventsAfterCheckpoint:  item.Details.WritePendingEventsAfterCheckpoint,
		}

		projs = append(projs, proj)
	}
}
