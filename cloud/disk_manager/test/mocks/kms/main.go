package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/ydb/public/api/client/yc_private/kms"
	"google.golang.org/grpc"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type kmsService struct {
}

func (s *kmsService) Encrypt(
	ctx context.Context,
	req *kms.SymmetricEncryptRequest,
) (*kms.SymmetricEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"Encrypt not implemented",
	)
}

func (s *kmsService) Decrypt(
	ctx context.Context,
	req *kms.SymmetricDecryptRequest,
) (*kms.SymmetricDecryptResponse, error) {

	log.Printf("get Decrypt request: %v", req)
	return &kms.SymmetricDecryptResponse{
		KeyId:     req.KeyId,
		VersionId: req.KeyId + "-version-id",
		Plaintext: []byte("12345678901234567890123456789012"),
	}, nil
}

func (s *kmsService) BatchEncrypt(
	ctx context.Context,
	req *kms.SymmetricBatchEncryptRequest,
) (*kms.SymmetricBatchEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"BatchEncrypt not implemented",
	)
}

func (s *kmsService) BatchDecrypt(
	ctx context.Context,
	req *kms.SymmetricBatchDecryptRequest,
) (*kms.SymmetricBatchDecryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"BatchDecrypt not implemented",
	)
}

func (s *kmsService) ReEncrypt(
	ctx context.Context,
	req *kms.SymmetricReEncryptRequest,
) (*kms.SymmetricReEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"ReEncrypt not implemented",
	)
}

func (s *kmsService) GenerateDataKey(
	ctx context.Context,
	req *kms.GenerateDataKeyRequest,
) (*kms.GenerateDataKeyResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"GenerateDataKey not implemented",
	)
}

func (s *kmsService) GenerateRandom(
	ctx context.Context,
	req *kms.GenerateRandomRequest,
) (*kms.GenerateRandomResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"GenerateRandom not implemented",
	)
}

func (s *kmsService) GenerateAsymmetricDataKey(
	ctx context.Context,
	req *kms.GenerateAsymmetricDataKeyRequest,
) (*kms.GenerateAsymmetricDataKeyResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"GenerateAsymmetricDataKey not implemented",
	)
}

////////////////////////////////////////////////////////////////////////////////

func run(port uint32) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on %v: %w", port, err)
	}

	log.Printf("listening on %v", lis.Addr().String())

	srv := grpc.NewServer()
	kms.RegisterSymmetricCryptoServiceServer(srv, &kmsService{})

	return srv.Serve(lis)
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("launching kms-mock")

	var port uint32
	var rootCmd = &cobra.Command{
		Use:   "kms-mock",
		Short: "Mock for KMS",
		Run: func(cmd *cobra.Command, args []string) {
			err := run(port)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}
	rootCmd.Flags().Uint32Var(&port, "port", 4202, "server port")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
