package access_service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	iamv1 "github.com/ydb-platform/nbs/cloud/storage/core/tools/testing/access_service_nebius/service/protos"
	mockConfig "github.com/ydb-platform/nbs/cloud/storage/core/tools/testing/access_service_nebius/service/config"
)

////////////////////////////////////////////////////////////////////////////////

type accessServiceMock struct {
	accountsByToken map[string]mockConfig.AccountConfig
}

func newAccessServiceMock(config mockConfig.MockConfig) *accessServiceMock {
	accountsByToken := make(map[string]mockConfig.AccountConfig)
	for _, accountConfig := range config.Accounts {
		accountsByToken[accountConfig.Token] = accountConfig
	}
	return &accessServiceMock{accountsByToken: accountsByToken}
}

func (t *accessServiceMock) Authorize(_ context.Context, request *iamv1.AuthorizeRequest) (*iamv1.AuthorizeResponse, error) {
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

		accountConfig, ok := t.accountsByToken[token]
		if !ok {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_INVALID_TOKEN,
				Account:    nil,
			}
			continue
		}

		account := &iamv1.Account{
			Type: &iamv1.Account_UserAccount_{
				UserAccount: &iamv1.Account_UserAccount{
					Id: accountConfig.Id,
				},
			},
		}
		if accountConfig.IsUnknownSubject {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_UNKNOWN_SUBJECT,
				Account:    account,
			}
			continue
		}

		ok = t.checkPermissions(accountConfig.Permissions, value)
		if !ok {
			results[key] = &iamv1.AuthorizeResult{
				ResultCode: iamv1.AuthorizeResult_PERMISSION_DENIED,
				Account:    account,
			}
			continue
		}

		results[key] = &iamv1.AuthorizeResult{
			ResultCode: iamv1.AuthorizeResult_OK,
			Account:    account,
		}
	}
	return &iamv1.AuthorizeResponse{Results: results}, nil
}

func (t *accessServiceMock) Authenticate(_ context.Context, request *iamv1.AuthenticateRequest) (*iamv1.AuthenticateResponse, error) {
	token := request.GetIamToken()
	if token == "" {
		return &iamv1.AuthenticateResponse{
			ResultCode: iamv1.AuthenticateResponse_INVALID_TOKEN,
			Account:    nil,
		}, nil
	}

	accountConfig, ok := t.accountsByToken[token]
	if !ok {
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
					Id: accountConfig.Id,
				},
			},
		},
	}, nil
}

func (t *accessServiceMock) checkPermissions(permissions []mockConfig.Permission, value *iamv1.AuthorizeCheck) bool {
	for _, permission := range permissions {
		if permission.Permission == value.Permission.Name && permission.Resource == value.GetContainerId() {
			return true
		}
	}
	return false
}

////////////////////////////////////////////////////////////////////////////////

func StartAccessService(configPath string) error {
	var config mockConfig.MockConfig
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	accessService := newAccessServiceMock(config)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		log.Fatalf("failed to listen on %v: %v", config.Port, err)
	}

	log.Printf("listening on %v", listener.Addr().String())
	creds, err := credentials.NewServerTLSFromFile(
		config.CertFile,
		config.CertKeyFile,
	)
	if err != nil {
		log.Fatalf("failed to parse certificates %v", err)
	}

	server := grpc.NewServer(grpc.Creds(creds))
	iamv1.RegisterAccessServiceServer(server, accessService)
	return server.Serve(listener)
}
