package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/user_management"
	"github.com/stretchr/testify/require"
)

func Test_CreateNewUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	userManagement := clientInstance.UserManagement()

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()

	err := userManagement.CreateUser(context.Background(), user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	})

	require.NoError(t, err)

	user, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)
	require.Equal(t, loginName, user.LoginName)
	require.Equal(t, "Full Name", user.FullName)
	require.Equal(t, []string{"foo", "bar"}, user.Groups)
	require.Equal(t, false, user.Disabled)
}

func Test_UpdateUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	t.Run("Existing user", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		request.FullName = "Full name 2"
		request.Groups = []string{"some", "other"}
		request.Password = "other password"

		err = userManagement.UpdateUser(context.Background(), request)
		require.NoError(t, err)

		user, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, loginName, user.LoginName)
		require.Equal(t, request.FullName, user.FullName)
		require.Equal(t, request.Groups, user.Groups)
		require.Equal(t, false, user.Disabled)
	})

	t.Run("Fails for non-existing user", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}

		err := userManagement.UpdateUser(context.Background(), request)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})
}

func Test_DeleteUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	t.Run("Delete Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)
	})

	t.Run("Delete Updated User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		request.FullName = "Full name 2"
		request.Groups = []string{"some", "other"}
		request.Password = "other password"

		err = userManagement.UpdateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)
	})

	t.Run("Fetching Deleted User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)

		_, err = userManagement.GetUserDetails(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Deleting Non-Existing User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		err := userManagement.DeleteUser(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})
}

func Test_EnableAndDisableUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	t.Run("Disable Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		user, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, true, user.Disabled)
	})

	t.Run("Disable Non-Existing User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		err := userManagement.DisableUser(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Disable Deleted User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Disable Already Disabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		user, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, true, user.Disabled)
	})

	t.Run("Disable Enabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.NoError(t, err)

		user, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, false, user.Disabled)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		user, err = userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, true, user.Disabled)
	})

	t.Run("Enable Non-Existing User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		err := userManagement.EnableUser(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Enable Deleted User Fails", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Enable Already Enabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.NoError(t, err)

		user, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)
		require.Equal(t, false, user.Disabled)
	})
}

func Test_ChangeUserPassword(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	t.Run("Succeeds For Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)
	})

	t.Run("Fails For Wrong Current Password", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Wrong Current Password",
				NewPassword:     "New Password",
			})
		require.Equal(t, errors.PermissionDeniedErr, err.Code())
	})

	t.Run("Fails For Non-Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()

		err := userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Wrong Current Password",
				NewPassword:     "New Password",
			})
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Is Idempotent", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "Password",
			})
		require.NoError(t, err)
	})

	t.Run("Can Be Executed Multiple Times", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "New Password",
				NewPassword:     "New Password 2",
			})
		require.NoError(t, err)
	})

	t.Run("Fails For Deleted User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Succeeds For Disabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)
	})

	t.Run("Succeeds For Re-Enabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)
	})

	t.Run("Succeeds After Reset User Password With Latest Current Password", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "New Password",
				NewPassword:     "New Password 2",
			})
		require.NoError(t, err)
	})

	t.Run("Fails After Reset User Password With Wrong Latest Current Password", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Wrong Current Password",
				NewPassword:     "New Password 2",
			})
		require.Equal(t, errors.PermissionDeniedErr, err.Code())
	})
}

func Test_ResetUserPassword(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	t.Run("Succeeds For Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.NoError(t, err)
	})

	t.Run("Fails For Non-Existing User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()

		err := userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Fails For Deleted User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DeleteUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.Equal(t, errors.UserNotFoundErr, err.Code())
	})

	t.Run("Succeeds For Disabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.NoError(t, err)
	})

	t.Run("Succeeds For Re-Enabled User", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.DisableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.EnableUser(context.Background(), loginName)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password")
		require.NoError(t, err)
	})

	t.Run("Succeeds After Change User Password", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password 2")
		require.NoError(t, err)
	})

	t.Run("Can Be Called Multiple Times", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password 2")
		require.NoError(t, err)

		err = userManagement.ResetUserPassword(context.Background(),
			loginName, "New Password 3")
		require.NoError(t, err)
	})
}

func Test_ListUsers_ListDefaultUsers(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	require.ElementsMatch(t, defaultUsers, result)
}

func Test_ListUsers_ListDefaultAndNewUsers(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)

	createdUser.LastUpdated = timeNow

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	expectedUsers := append(defaultUsers, createdUser)
	require.ElementsMatch(t, expectedUsers, result)
}

func Test_ListUsers_DoNotListDeletedUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	err = userManagement.DeleteUser(context.Background(), loginName)
	require.NoError(t, err)

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	require.ElementsMatch(t, defaultUsers, result)
}

func Test_ListUsers_ListDisabledUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	err = userManagement.DisableUser(context.Background(), loginName)
	require.NoError(t, err)

	createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)

	createdUser.LastUpdated = timeNow

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	expectedUsers := append(defaultUsers, createdUser)
	require.ElementsMatch(t, expectedUsers, result)
}

func Test_ListUsers_ListReEnabledUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	err = userManagement.DisableUser(context.Background(), loginName)
	require.NoError(t, err)

	err = userManagement.EnableUser(context.Background(), loginName)
	require.NoError(t, err)

	createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)

	createdUser.LastUpdated = timeNow

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	expectedUsers := append(defaultUsers, createdUser)
	require.ElementsMatch(t, expectedUsers, result)
}

func Test_ListUsers_ListsUserWithChangedPassword(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	t.Run("Lists User With Changed Password", func(t *testing.T) {
		newUUID, _ := uuid.NewV4()
		loginName := newUUID.String()
		request := user_management.CreateOrUpdateRequest{
			LoginName: loginName,
			Password:  "Password",
			FullName:  "Full Name",
			Groups:    []string{"foo", "bar"},
		}
		err := userManagement.CreateUser(context.Background(), request)
		require.NoError(t, err)

		err = userManagement.ChangeUserPassword(context.Background(),
			user_management.ChangePasswordRequest{
				LoginName:       loginName,
				CurrentPassword: "Password",
				NewPassword:     "New Password",
			})
		require.NoError(t, err)

		createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
		require.NoError(t, err)

		createdUser.LastUpdated = timeNow

		allUsers, err := userManagement.ListAllUsers(context.Background())
		require.NoError(t, err)

		var result []user_management.DetailsResponse

		for _, user := range allUsers {
			user.LastUpdated = timeNow
			result = append(result, user)
		}

		expectedUsers := append(defaultUsers, createdUser)
		require.ElementsMatch(t, expectedUsers, result)
	})
}

func Test_ListUsers_ListsUserWithResetPassword(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	err = userManagement.ResetUserPassword(context.Background(),
		loginName, "New Password")
	require.NoError(t, err)

	createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)

	createdUser.LastUpdated = timeNow

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	expectedUsers := append(defaultUsers, createdUser)
	require.ElementsMatch(t, expectedUsers, result)
}

func Test_ListUsers_ListsUpdatedUser(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()
	userManagement := clientInstance.UserManagement()

	timeNow := time.Now().UTC()
	defaultAdminUser := user_management.DetailsResponse{
		LoginName:   "admin",
		FullName:    "Event Store Administrator",
		Groups:      []string{"$admins"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultOpsUser := user_management.DetailsResponse{
		LoginName:   "ops",
		FullName:    "Event Store Operations",
		Groups:      []string{"$ops"},
		LastUpdated: timeNow,
		Disabled:    false,
	}

	defaultUsers := []user_management.DetailsResponse{
		defaultAdminUser, defaultOpsUser,
	}

	newUUID, _ := uuid.NewV4()
	loginName := newUUID.String()
	request := user_management.CreateOrUpdateRequest{
		LoginName: loginName,
		Password:  "Password",
		FullName:  "Full Name",
		Groups:    []string{"foo", "bar"},
	}
	err := userManagement.CreateUser(context.Background(), request)
	require.NoError(t, err)

	request.FullName = "New Full Name"
	request.Groups = []string{"new group"}
	request.Password = "New Password"

	err = userManagement.UpdateUser(context.Background(), request)
	require.NoError(t, err)

	createdUser, err := userManagement.GetUserDetails(context.Background(), loginName)
	require.NoError(t, err)

	createdUser.LastUpdated = timeNow

	allUsers, err := userManagement.ListAllUsers(context.Background())
	require.NoError(t, err)

	var result []user_management.DetailsResponse

	for _, user := range allUsers {
		user.LastUpdated = timeNow
		result = append(result, user)
	}

	expectedUsers := append(defaultUsers, createdUser)
	require.ElementsMatch(t, expectedUsers, result)
}
