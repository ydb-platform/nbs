package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/compute/config"
	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute"
	compute "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

////////////////////////////////////////////////////////////////////////////////

type computeService struct {
	compute.UnimplementedDiskServiceServer
	s3Credentials map[string]*api.ExportS3Credentials
}

func (s *computeService) CreateToken(
	ctx context.Context,
	req *compute.CreateTokenRequest,
) (*compute.CreateTokenResponse, error) {

	log.Printf("get CreateToken request: %v", req)
	return &compute.CreateTokenResponse{
		Token: "token-from-compute-mock",
	}, nil
}

func (s *computeService) GetExportCredentials(
	ctx context.Context,
	req *compute.GetExportCredentialsRequest,
) (*compute.GetExportCredentialsResponse, error) {

	key, err := exportCredentialsKey(req.GetSnapshotId(), req.GetImageId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if s.s3Credentials == nil {
		return nil, status.Error(
			codes.FailedPrecondition,
			"S3 credentials are not configured; use --config",
		)
	}

	credentials, ok := s.s3Credentials[key]
	if !ok {
		return nil, status.Error(
			codes.NotFound, "export credentials are not configured for this resource",
		)
	}

	// task_id identifies the caller's operation, not the credential entry.
	// Return a fresh message so concurrent RPCs do not share protobuf state.
	return &compute.GetExportCredentialsResponse{
		Credentials: &api.ExportS3Credentials{
			AccessKeyId:     credentials.GetAccessKeyId(),
			SecretAccessKey: credentials.GetSecretAccessKey(),
		},
	}, nil
}

func exportCredentialsKey(snapshotID, imageID string) (string, error) {
	if (snapshotID == "") == (imageID == "") {
		return "", fmt.Errorf("exactly one snapshot_id or image_id is required")
	}

	if snapshotID != "" {
		return "snapshot/" + snapshotID, nil
	}

	return "image/" + imageID, nil
}

////////////////////////////////////////////////////////////////////////////////

func parseConfig(path string) (*config.ComputeServiceMockConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config %v: %w", path, err)
	}

	serviceConfig := &config.ComputeServiceMockConfig{}
	err = prototext.Unmarshal(data, proto.MessageV2(serviceConfig))
	if err != nil {
		// Parser diagnostics can include the offending token from the file.
		// Never include credential values in command errors or logs.
		return nil, fmt.Errorf("invalid protobuf configuration in %v", path)
	}

	if serviceConfig.GetListenAddress() == "" {
		return nil, fmt.Errorf("ListenAddress must not be empty")
	}

	entries := serviceConfig.GetExportCredentials()
	if len(entries) == 0 {
		return nil, fmt.Errorf("at least one ExportCredentials entry is required")
	}

	seen := make(map[string]bool)
	for i, entry := range entries {
		key, err := exportCredentialsKey(entry.GetSnapshotId(), entry.GetImageId())
		if err != nil {
			return nil, fmt.Errorf("ExportCredentials[%v]: %v", i, err)
		}

		if seen[key] {
			return nil, fmt.Errorf("duplicate resource in ExportCredentials[%v]", i)
		}

		seen[key] = true
		credentials := entry.GetCredentials()
		if credentials.GetAccessKeyId() == "" ||
			credentials.GetSecretAccessKey() == "" {

			return nil, fmt.Errorf(
				"ExportCredentials[%v] requires both S3 credential fields", i,
			)
		}
	}

	return serviceConfig, nil
}

func run(
	ctx context.Context,
	address string,
	entries []*config.ExportCredentials,
) error {

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %v: %w", address, err)
	}
	defer lis.Close()

	log.Printf("listening on %v", lis.Addr().String())

	var credentials map[string]*api.ExportS3Credentials
	if len(entries) != 0 {
		credentials = make(map[string]*api.ExportS3Credentials, len(entries))
		for _, entry := range entries {
			key, err := exportCredentialsKey(entry.GetSnapshotId(), entry.GetImageId())
			if err != nil {
				return err
			}

			credentials[key] = entry.GetCredentials()
		}
	}

	srv := grpc.NewServer()
	compute.RegisterDiskServiceServer(srv, &computeService{
		s3Credentials: credentials,
	})

	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			srv.GracefulStop()
		case <-stopped:
		}
	}()

	err = srv.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("launching compute-mock")

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()

	var port uint32
	var configPath string
	rootCmd := &cobra.Command{
		Use:           "compute-mock",
		Short:         "Mock for Compute with snapshot export credentials",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Preserve --port for recipes that only need CreateToken.
			address := fmt.Sprintf(":%d", port)
			var entries []*config.ExportCredentials
			if configPath != "" {
				if cmd.Flags().Changed("port") {
					return fmt.Errorf(
						"set ListenAddress in the config instead of --port",
					)
				}

				serviceConfig, err := parseConfig(configPath)
				if err != nil {
					return err
				}

				address = serviceConfig.GetListenAddress()
				entries = serviceConfig.GetExportCredentials()
			}

			return run(cmd.Context(), address, entries)
		},
	}
	rootCmd.Flags().Uint32Var(&port, "port", 4201, "server port")
	rootCmd.Flags().StringVar(
		&configPath,
		"config",
		"",
		"Path to the protobuf config with S3 credentials",
	)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
