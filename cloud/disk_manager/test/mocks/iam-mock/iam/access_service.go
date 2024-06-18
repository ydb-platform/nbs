package iam

import (
	"context"
	"encoding/json"
	iamv1 "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/internal_auth/access_service"
	"net/http"
	"sync"
)

type Permission struct {
	Permission string `json:"permission,omitempty"`
	Resource   string `json:"resource,omitempty"`
}

type AddPermissionRequest struct {
	AccountName string       `json:"account_name,omitempty"`
	Permissions []Permission `json:"permissions,omitempty"`
}

type AccessServiceMock struct {
	mutex        sync.Mutex
	issuedTokens map[string]IssuedTokenType
	permissions  map[string][]Permission
	mux          *http.ServeMux
}

func NewAccessServiceMock(mux *http.ServeMux) *AccessServiceMock {
	service := &AccessServiceMock{
		mutex:        sync.Mutex{},
		issuedTokens: make(map[string]IssuedTokenType),
		permissions:  make(map[string][]Permission),
		mux:          mux,
	}
	service.registerHttpHandlers()
	return service
}

func (t *AccessServiceMock) AddIssuedToken(token IssuedTokenType) {
	t.issuedTokens[token.value] = token
}

func (t *AccessServiceMock) registerHttpHandlers() {
	t.mux.HandleFunc(
		"/permissions/add",
		func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				http.Error(
					writer,
					"Invalid method",
					http.StatusMethodNotAllowed,
				)
				return
			}
			decoder := json.NewDecoder(request.Body)
			var addPermissionRequest AddPermissionRequest
			err := decoder.Decode(&addPermissionRequest)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			t.AddPermission(addPermissionRequest)
		},
	)
}
func (t *AccessServiceMock) checkPermissions(permissions []Permission, value *iamv1.AuthorizeCheck) bool {
	for _, permission := range permissions {
		if permission.Permission == value.Permission.Name && permission.Resource == value.GetContainerId() {
			return true
		}
	}
	return false
}

func (t *AccessServiceMock) AddPermission(request AddPermissionRequest) {
	_, ok := t.permissions[request.AccountName]
	if !ok {
		t.permissions[request.AccountName] = request.Permissions
		return
	}

	t.permissions[request.AccountName] = append(t.permissions[request.AccountName], request.Permissions...)
}

func (t *AccessServiceMock) Authorize(_ context.Context, request *iamv1.AuthorizeRequest) (*iamv1.AuthorizeResponse, error) {
	results := make(map[int64]*iamv1.AuthorizeResult)
	for key, value := range request.GetChecks() {
		token := value.GetIamToken()
		if token == "" {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_INVALID_TOKEN,
				Account:    nil,
			}
			continue
		}

		issuedToken, ok := t.issuedTokens[token]
		if !ok {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_INVALID_TOKEN,
				Account:    nil,
			}
			continue
		}

		if !issuedToken.check() {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_INVALID_TOKEN,
				Account:    nil,
			}
			continue
		}

		accountId := issuedToken.subject
		permissions, ok := t.permissions[accountId]
		if !ok {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_UNKNOWN_SUBJECT,
				Account:    nil,
			}
			continue
		}

		ok = t.checkPermissions(permissions, value)
		if !ok {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_PERMISSION_DENIED,
				Account:    nil,
			}
			continue
		}

		results[key] = &iamv1.AuthorizeResult{
			ResultCode: iamv1.AuthorizeResult_OK,
			Account: &iamv1.Account{
				Type: &iamv1.Account_UserAccount_{
					UserAccount: &iamv1.Account_UserAccount{
						Id: accountId,
					},
				},
			},
		}
	}
	return &iamv1.AuthorizeResponse{Results: results}, nil
}

func (t *AccessServiceMock) Authenticate(_ context.Context, request *iamv1.AuthenticateRequest) (*iamv1.AuthenticateResponse, error) {
	token := request.GetIamToken()
	if token == "" {
		return &iamv1.AuthenticateResponse{
			ResultCode: iamv1.AuthenticateResponse_INVALID_TOKEN,
			Account:    nil,
		}, nil
	}

	issuedToken, ok := t.issuedTokens[token]
	if !ok {
		return &iamv1.AuthenticateResponse{
			ResultCode: iamv1.AuthenticateResponse_INVALID_TOKEN,
			Account:    nil,
		}, nil
	}

	if !issuedToken.check() {
		return &iamv1.AuthenticateResponse{
			ResultCode: iamv1.AuthenticateResponse_INVALID_TOKEN,
			Account:    nil,
		}, nil
	}

	return &iamv1.AuthenticateResponse{
		ResultCode: iamv1.AuthenticateResponse_OK,
		Account: &iamv1.Account{
			Type: &iamv1.Account_UserAccount_{
				UserAccount: &iamv1.Account_UserAccount{
					Id: issuedToken.subject,
				},
			},
		},
	}, nil
}
