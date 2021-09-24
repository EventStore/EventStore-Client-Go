package user_management

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientImpl struct {
	grpcClient            connection.GrpcClient
	grpcUserClientFactory grpcUserClientFactory
	detailsReaderFactory  DetailsReaderFactory
}

func newClientImpl(grpcClient connection.GrpcClient,
	grpcUserClientFactory grpcUserClientFactory,
	detailsReaderFactory DetailsReaderFactory) *ClientImpl {
	return &ClientImpl{
		grpcClient:            grpcClient,
		grpcUserClientFactory: grpcUserClientFactory,
		detailsReaderFactory:  detailsReaderFactory,
	}
}

func (client *ClientImpl) CreateUser(ctx context.Context, request CreateOrUpdateRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.Create(ctx, request.BuildCreateRequest(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) UpdateUser(ctx context.Context, request CreateOrUpdateRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.Update(ctx, request.BuildUpdateRequest(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) DeleteUser(ctx context.Context, loginName string) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.Delete(ctx, DeleteRequest(loginName).Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) DisableUser(ctx context.Context, loginName string) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.Disable(ctx, DisableRequest(loginName).Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) EnableUser(ctx context.Context, loginName string) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.Enable(ctx, EnableRequest(loginName).Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) GetUserDetails(ctx context.Context, loginName string) (DetailsResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return DetailsResponse{}, err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	protoStreamReader, protoErr := grpcUsersClient.Details(ctx, DetailsRequest(loginName).Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return DetailsResponse{}, err
	}

	userDetailsReader := client.detailsReaderFactory.Create(protoStreamReader, cancel)

	return userDetailsReader.Recv()
}

func (client *ClientImpl) ListAllUsers(ctx context.Context) ([]DetailsResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	protoStreamReader, protoErr := grpcUsersClient.Details(ctx, DetailsRequest(AllUsers).Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return nil, err
	}

	userDetailsReader := client.detailsReaderFactory.Create(protoStreamReader, cancel)

	var result []DetailsResponse
	for {
		response, readerErr := userDetailsReader.Recv()
		if readerErr != nil {
			if readerErr.Code() == errors.EndOfStream {
				break
			}
			return nil, readerErr
		}

		result = append(result, response)
	}

	return result, nil
}

func (client *ClientImpl) ChangeUserPassword(ctx context.Context, request ChangePasswordRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.ChangePassword(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}

func (client *ClientImpl) ResetUserPassword(ctx context.Context, loginName string, newPassword string) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	grpcUsersClient := client.grpcUserClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := grpcUsersClient.ResetPassword(ctx, ResetPasswordRequest{
		LoginName:   loginName,
		NewPassword: newPassword,
	}.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			errors.FatalError)
		return err
	}

	return nil
}
