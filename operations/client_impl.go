package operations

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/operations"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type clientImpl struct {
	grpcClient                  connection.GrpcClient
	scavengeResponseAdapter     scavengeResponseAdapter
	grpcOperationsClientFactory grpcOperationsClientFactory
}

func newClientImpl(grpcClient connection.GrpcClient,
	scavengeResponseAdapter scavengeResponseAdapter,
	grpcOperationsClientFactory grpcOperationsClientFactory) *clientImpl {
	return &clientImpl{
		grpcClient:                  grpcClient,
		scavengeResponseAdapter:     scavengeResponseAdapter,
		grpcOperationsClientFactory: grpcOperationsClientFactory,
	}
}

func (client *clientImpl) Shutdown(ctx context.Context) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcOperationsClient.Shutdown(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, errors.FatalError)
		return err
	}

	return nil
}

const MergeIndexesErr errors.ErrorCode = "MergeIndexesErr"

func (client *clientImpl) MergeIndexes(ctx context.Context) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcOperationsClient.MergeIndexes(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, MergeIndexesErr)
		return err
	}

	return nil
}

func (client *clientImpl) ResignNode(ctx context.Context) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcOperationsClient.ResignNode(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, errors.FatalError)
		return err
	}

	return nil
}

func (client *clientImpl) SetNodePriority(ctx context.Context, priority int32) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	request := &operations.SetNodePriorityReq{
		Priority: priority,
	}

	var headers, trailers metadata.MD
	_, protoErr := grpcOperationsClient.SetNodePriority(ctx, request,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, errors.FatalError)
		return err
	}

	return nil
}

func (client *clientImpl) RestartPersistentSubscriptions(ctx context.Context) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcOperationsClient.RestartPersistentSubscriptions(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

const (
	StartScavenge_ThreadCountLessOrEqualZeroErr errors.ErrorCode = "StartScavenge_ThreadCountLessOrEqualZeroErr"
	StartScavenge_StartFromChunkLessThanZeroErr errors.ErrorCode = "StartScavenge_StartFromChunkLessThanZeroErr"
)

func (client *clientImpl) StartScavenge(ctx context.Context,
	request StartScavengeRequest) (ScavengeResponse, errors.Error) {

	if request.ThreadCount <= 0 {
		return ScavengeResponse{}, errors.NewErrorCode(StartScavenge_ThreadCountLessOrEqualZeroErr)
	}

	if request.StartFromChunk < 0 {
		return ScavengeResponse{}, errors.NewErrorCode(StartScavenge_StartFromChunkLessThanZeroErr)
	}

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return ScavengeResponse{}, err
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	protoResponse, protoErr := grpcOperationsClient.StartScavenge(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return ScavengeResponse{}, err
	}

	return client.scavengeResponseAdapter.create(protoResponse), nil
}

func (client *clientImpl) StopScavenge(ctx context.Context,
	scavengeId string) (ScavengeResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return ScavengeResponse{}, err
	}

	request := &operations.StopScavengeReq{
		Options: &operations.StopScavengeReq_Options{
			ScavengeId: scavengeId,
		},
	}

	grpcOperationsClient := client.grpcOperationsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	protoResponse, protoErr := grpcOperationsClient.StopScavenge(ctx, request,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return ScavengeResponse{}, err
	}

	return client.scavengeResponseAdapter.create(protoResponse), nil
}
