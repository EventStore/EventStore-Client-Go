package user_management

import (
	"strings"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/protos/users"
)

type CreateOrUpdateRequest struct {
	LoginName string
	Password  string
	FullName  string
	Groups    []string
}

func (request CreateOrUpdateRequest) BuildCreateRequest() *users.CreateReq {
	return &users.CreateReq{
		Options: &users.CreateReq_Options{
			LoginName: request.LoginName,
			Password:  request.Password,
			FullName:  request.FullName,
			Groups:    request.Groups,
		},
	}
}

func (request CreateOrUpdateRequest) BuildUpdateRequest() *users.UpdateReq {
	return &users.UpdateReq{
		Options: &users.UpdateReq_Options{
			LoginName: request.LoginName,
			Password:  request.Password,
			FullName:  request.FullName,
			Groups:    request.Groups,
		},
	}
}

type DeleteRequest string

func (request DeleteRequest) Build() *users.DeleteReq {
	return &users.DeleteReq{
		Options: &users.DeleteReq_Options{
			LoginName: string(request),
		},
	}
}

type DisableRequest string

func (request DisableRequest) Build() *users.DisableReq {
	return &users.DisableReq{
		Options: &users.DisableReq_Options{
			LoginName: string(request),
		},
	}
}

type EnableRequest string

func (request EnableRequest) Build() *users.EnableReq {
	return &users.EnableReq{
		Options: &users.EnableReq_Options{
			LoginName: string(request),
		},
	}
}

type DetailsRequest string

const AllUsers = ""

func (request DetailsRequest) Build() *users.DetailsReq {
	if (strings.TrimSpace(string(request))) == "" {
		return &users.DetailsReq{}
	}
	return &users.DetailsReq{
		Options: &users.DetailsReq_Options{
			LoginName: string(request),
		},
	}
}

type DetailsResponse struct {
	LoginName   string
	FullName    string
	Groups      []string
	LastUpdated time.Time
	Disabled    bool
}

type detailsResponseAdapter interface {
	Create(proto *users.DetailsResp) DetailsResponse
}

type detailsResponseAdapterImpl struct{}

func (adapter detailsResponseAdapterImpl) Create(proto *users.DetailsResp) DetailsResponse {
	return DetailsResponse{
		LoginName:   proto.UserDetails.LoginName,
		FullName:    proto.UserDetails.FullName,
		Groups:      proto.UserDetails.Groups,
		LastUpdated: time.Unix(0, proto.UserDetails.LastUpdated.TicksSinceEpoch*100).UTC(),
		Disabled:    proto.UserDetails.Disabled,
	}
}

type ChangePasswordRequest struct {
	LoginName       string
	CurrentPassword string
	NewPassword     string
}

func (request ChangePasswordRequest) Build() *users.ChangePasswordReq {
	return &users.ChangePasswordReq{
		Options: &users.ChangePasswordReq_Options{
			LoginName:       request.LoginName,
			CurrentPassword: request.CurrentPassword,
			NewPassword:     request.NewPassword,
		},
	}
}

type ResetPasswordRequest struct {
	LoginName   string
	NewPassword string
}

func (request ResetPasswordRequest) Build() *users.ResetPasswordReq {
	return &users.ResetPasswordReq{
		Options: &users.ResetPasswordReq_Options{
			LoginName:   request.LoginName,
			NewPassword: request.NewPassword,
		},
	}
}
