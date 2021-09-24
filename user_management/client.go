package user_management

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	CreateUser(ctx context.Context, request CreateOrUpdateRequest) errors.Error
	UpdateUser(ctx context.Context, request CreateOrUpdateRequest) errors.Error
	DeleteUser(ctx context.Context, loginName string) errors.Error
	DisableUser(ctx context.Context, loginName string) errors.Error
	EnableUser(ctx context.Context, loginName string) errors.Error
	GetUserDetails(ctx context.Context, loginName string) (DetailsResponse, errors.Error)
	ChangeUserPassword(ctx context.Context, request ChangePasswordRequest) errors.Error
	ResetUserPassword(ctx context.Context, loginName string, newPassword string) errors.Error
	ListAllUsers(ctx context.Context) ([]DetailsResponse, errors.Error)
}

type DetailsReader interface {
	Recv() (DetailsResponse, errors.Error)
}
