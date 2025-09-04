package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/spf13/cobra"
	iamv1 "github.com/ydb-platform/nbs/cloud/storage/core/tools/testing/access_service_new/mock/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

////////////////////////////////////////////////////////////////////////////////

type Permission struct {
	Permission string `json:"permission,omitempty"`
	Resource   string `json:"resource,omitempty"`
}

type AccountConfig struct {
	Permissions      []Permission `json:"permissions,omitempty"`
	ID               string       `json:"id,omitempty"`
	IsUnknownSubject bool         `json:"is_unknown_subject,omitempty"`
	Token            string       `json:"token,omitempty"`
}

type MockConfig struct {
	Host        string `json:"host,omitempty"`
	Port        int    `json:"port,omitempty"`
	ControlPort int    `json:"control_port,omitempty"`
	CertFile    string `json:"cert_file,omitempty"`
	CertKeyFile string `json:"cert_key_file,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////

type accessServiceMock struct {
	mu              *sync.Mutex
	accountsByToken map[string]AccountConfig
}

func newAccessServiceMock() *accessServiceMock {
	return &accessServiceMock{
		accountsByToken: make(map[string]AccountConfig),
		mu:              &sync.Mutex{},
	}
}

func (t *accessServiceMock) HandleCreateAccount(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var accountConfig AccountConfig
	err := json.NewDecoder(r.Body).Decode(&accountConfig)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	t.addAccount(accountConfig)
	w.WriteHeader(http.StatusOK)
}

func (t *accessServiceMock) Authorize(
	_ context.Context,
	request *iamv1.AuthorizeRequest,
) (*iamv1.AuthorizeResponse, error) {

	log.Printf("Received authorize request %v\n", request)
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

		accountConfig, ok := t.getAccount(token)
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
					Id: accountConfig.ID,
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

func (t *accessServiceMock) Authenticate(
	_ context.Context,
	request *iamv1.AuthenticateRequest,
) (*iamv1.AuthenticateResponse, error) {

	log.Printf("Received authenticate request %v\n", request)
	token := request.GetIamToken()
	if token == "" {
		return &iamv1.AuthenticateResponse{
			ResultCode: iamv1.AuthenticateResponse_INVALID_TOKEN,
			Account:    nil,
		}, nil
	}

	accountConfig, ok := t.getAccount(token)
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
					Id: accountConfig.ID,
				},
			},
		},
	}, nil
}

func (t *accessServiceMock) checkPermissions(
	permissions []Permission,
	value *iamv1.AuthorizeCheck,
) bool {

	for _, permission := range permissions {
		nameMatches := permission.Permission == value.Permission.Name
		containerMatches := permission.Resource == value.GetContainerId()
		if nameMatches && containerMatches {
			return true
		}
	}
	return false
}

func (t *accessServiceMock) addAccount(config AccountConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.accountsByToken[config.Token] = config
}

func (t *accessServiceMock) getAccount(token string) (AccountConfig, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	account, ok := t.accountsByToken[token]
	return account, ok
}

////////////////////////////////////////////////////////////////////////////////

func StartAccessService(configPath string) error {
	var config MockConfig
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	accessService := newAccessServiceMock()
	listener, err := net.Listen(
		"tcp",
		fmt.Sprintf("%s:%d", config.Host, config.Port),
	)
	if err != nil {
		log.Fatalf("failed to listen on %v: %v", config.Port, err)
	}

	log.Printf("listening on %v", listener.Addr().String())
	var creds credentials.TransportCredentials
	creds = insecure.NewCredentials()
	if config.CertFile != "" && config.CertKeyFile != "" {
		creds, err = credentials.NewServerTLSFromFile(
			config.CertFile,
			config.CertKeyFile,
		)
		if err != nil {
			log.Fatalf("failed to parse certificates %v", err)
		}
	}

	http.HandleFunc("/", accessService.HandleCreateAccount)
	go func() {
		err := http.ListenAndServe(
			fmt.Sprintf("%s:%d", config.Host, config.ControlPort),
			nil,
		)
		if err != nil {
			log.Fatalf(
				"Error while serving access service mock control api %v",
				err,
			)
		}
	}()
	server := grpc.NewServer(grpc.Creds(creds))
	iamv1.RegisterAccessServiceServer(server, accessService)
	return server.Serve(listener)
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configPath string
	rootCmd := &cobra.Command{
		Use: "Access Service Mock",
		RunE: func(cmd *cobra.Command, args []string) error {
			return StartAccessService(configPath)
		},
	}

	rootCmd.Flags().StringVar(&configPath, "config-path", "", "Path to a config")
	if err := rootCmd.MarkFlagRequired("config-path"); err != nil {
		log.Fatalf("Config path is required: %v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error while starting server: %v", err)
	}
}
