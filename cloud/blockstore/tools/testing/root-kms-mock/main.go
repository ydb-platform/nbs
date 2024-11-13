package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	_ "encoding/pem"
	"fmt"
	_ "io/ioutil"
	"log"
	"net"
	"os"
	"sync"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/kms"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type MockConfig struct {
	Port uint32    `json:"port"`
	TLS  TLSConfig `json:"tls"`
	Keys []string  `json:"keys"`
}

type TLSConfig struct {
	CertFile string `json:"cert_path"`
	KeyFile  string `json:"key_path"`
	CAFile   string `json:"ca_path"`
}

////////////////////////////////////////////////////////////////////////////////

type rootKmsService struct {
	keys map[string]*rsa.PrivateKey
	mtx  sync.RWMutex
}

func (s *rootKmsService) generateKeys(keys []string) error {
	for _, keyID := range keys {
		log.Printf("generating RSA 4096 for %q", keyID)
		privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
		if err != nil {
			return fmt.Errorf("error generating key: %v\n", err)
		}
		s.keys[keyID] = privateKey
	}

	return nil
}

func generateAndEncryptDEK(publicKey *rsa.PublicKey) ([]byte, error) {

	symmetricKey := make([]byte, 32)
	_, err := rand.Read(symmetricKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DEK: %w", err)
	}

	if len(symmetricKey) != 32 {
		return nil, fmt.Errorf("incorrect key length: %d", len(symmetricKey))
	}

	encryptedKey, err := rsa.EncryptOAEP(
		sha256.New(),
		rand.Reader,
		publicKey,
		symmetricKey,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("symmetric key encryption error: %w", err)
	}

	return encryptedKey, nil
}

func decryptDEK(
	encryptedKey []byte,
	privateKey *rsa.PrivateKey,
) ([]byte, error) {

	symmetricKey, err := rsa.DecryptOAEP(
		sha256.New(),
		rand.Reader,
		privateKey,
		encryptedKey,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("symmetric key decryption error: %w", err)
	}

	if len(symmetricKey) != 32 {
		return nil, fmt.Errorf(
			"incorrect decrypted key length: %d",
			len(symmetricKey),
		)
	}

	return symmetricKey, nil
}

func (s *rootKmsService) Encrypt(
	ctx context.Context,
	req *kms.SymmetricEncryptRequest,
) (*kms.SymmetricEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"Encrypt not implemented",
	)
}

func (s *rootKmsService) Decrypt(
	ctx context.Context,
	req *kms.SymmetricDecryptRequest,
) (*kms.SymmetricDecryptResponse, error) {

	log.Printf("Decrypt request: %v", req)

	s.mtx.RLock()
	privateKey, exists := s.keys[req.KeyId]
	s.mtx.RUnlock()

	if !exists {
		return nil, grpc_status.Error(
			grpc_codes.NotFound,
			fmt.Sprintf("Key %q not found", req.KeyId),
		)
	}

	symmetricKey, err := decryptDEK(
		req.Ciphertext,
		privateKey,
	)
	if err != nil {
		return nil, grpc_status.Error(
			grpc_codes.Internal,
			fmt.Sprintf("symmetric key decryption error: %w", err),
		)
	}

	return &kms.SymmetricDecryptResponse{
		KeyId:     req.KeyId,
		Plaintext: symmetricKey,
	}, nil
}

func (s *rootKmsService) BatchEncrypt(
	ctx context.Context,
	req *kms.SymmetricBatchEncryptRequest,
) (*kms.SymmetricBatchEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"BatchEncrypt not implemented",
	)
}

func (s *rootKmsService) BatchDecrypt(
	ctx context.Context,
	req *kms.SymmetricBatchDecryptRequest,
) (*kms.SymmetricBatchDecryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"BatchDecrypt not implemented",
	)
}

func (s *rootKmsService) ReEncrypt(
	ctx context.Context,
	req *kms.SymmetricReEncryptRequest,
) (*kms.SymmetricReEncryptResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"ReEncrypt not implemented",
	)
}

func (s *rootKmsService) GenerateDataKey(
	ctx context.Context,
	req *kms.GenerateDataKeyRequest,
) (*kms.GenerateDataKeyResponse, error) {

	log.Printf("GenerateDataKey request: %v", req)

	if !req.SkipPlaintext {
		return nil, grpc_status.Error(
			grpc_codes.InvalidArgument,
			"!SkipPlaintext",
		)
	}

	if req.DataKeySpec != kms.SymmetricAlgorithm_AES_256 {
		return nil, grpc_status.Error(
			grpc_codes.InvalidArgument,
			"DataKeySpec != AES_256",
		)
	}

	s.mtx.RLock()
	privateKey, exists := s.keys[req.KeyId]
	s.mtx.RUnlock()

	if !exists {
		return nil, grpc_status.Error(
			grpc_codes.NotFound,
			fmt.Sprintf("Key %q not found", req.KeyId),
		)
	}

	ciphertext, err := generateAndEncryptDEK(&privateKey.PublicKey)
	if err != nil {
		return nil, grpc_status.Error(
			grpc_codes.Internal,
			fmt.Sprintf("failed to encrypt DEK: %v", err),
		)
	}

	return &kms.GenerateDataKeyResponse{
		KeyId:             req.KeyId,
		DataKeyCiphertext: ciphertext,
	}, nil
}

func (s *rootKmsService) GenerateRandom(
	ctx context.Context,
	req *kms.GenerateRandomRequest,
) (*kms.GenerateRandomResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"GenerateRandom not implemented",
	)
}

func (s *rootKmsService) GenerateAsymmetricDataKey(
	ctx context.Context,
	req *kms.GenerateAsymmetricDataKeyRequest,
) (*kms.GenerateAsymmetricDataKeyResponse, error) {

	return nil, grpc_status.Error(
		grpc_codes.Unimplemented,
		"GenerateAsymmetricDataKey not implemented",
	)
}

////////////////////////////////////////////////////////////////////////////////

func loadTLSCredentials(config TLSConfig) (*tls.Config, error) {

	certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %v", err)
	}

	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("could not read CA certificate: %v", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, fmt.Errorf("failed to append CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
	}

	return tlsConfig, nil
}

////////////////////////////////////////////////////////////////////////////////

func run(configPath string) error {

	var config MockConfig
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	tlsCreds, err := loadTLSCredentials(config.TLS)
	if err != nil {
		log.Fatalf("failed to load TLS credentials: %v", err)
	}

	log.Printf("launching Root KMS mock on port %d\n", config.Port)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on %v: %w", config.Port, err)
	}

	log.Printf("listening on %v", lis.Addr().String())

	s := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCreds)),
	)

	rootKms := &rootKmsService{
		keys: make(map[string]*rsa.PrivateKey),
	}
	if err := rootKms.generateKeys(config.Keys); err != nil {
		return fmt.Errorf("failed to generate keys: %v", err)
	}

	kms.RegisterSymmetricCryptoServiceServer(s, rootKms)

	return s.Serve(lis)
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configPath string
	var rootCmd = &cobra.Command{
		Use:   "root-kms-mock",
		Short: "Mock for RootKMS",
		Run: func(cmd *cobra.Command, args []string) {
			err := run(configPath)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}
	rootCmd.Flags().StringVar(&configPath, "config-path", "", "Path to a config")
	if err := rootCmd.MarkFlagRequired("config-path"); err != nil {
		log.Fatalf("Config path is required: %v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
