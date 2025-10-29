package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/ydb/public/api/client/yc_private/kms"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type Config struct {
	Port       uint32            `json:"port"`
	RootCAPath string            `json:"ca"`
	CertPath   string            `json:"server_cert"`
	KeyPath    string            `json:"server_key"`
	Keys       map[string]string `json:"keys"`
}

////////////////////////////////////////////////////////////////////////////////

type rootKmsService struct {
	kms.UnimplementedSymmetricCryptoServiceServer
	keys map[string]*rsa.PrivateKey
	mtx  sync.RWMutex
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

	symmetricKey, err := decryptDEK(req.Ciphertext, privateKey)
	if err != nil {
		return nil, grpc_status.Error(
			grpc_codes.Internal,
			fmt.Sprintf("symmetric key decryption error: %v", err),
		)
	}

	return &kms.SymmetricDecryptResponse{
		KeyId:     req.KeyId,
		Plaintext: symmetricKey,
	}, nil
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

func (s *rootKmsService) loadKeys(config Config) error {

	for keyID, path := range config.Keys {
		privateKeyBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		block, _ := pem.Decode(privateKeyBytes)
		if block == nil {
			return fmt.Errorf("failed to parse PEM block for the key %q", keyID)
		}

		privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return err
		}

		rsaKey, ok := privateKey.(*rsa.PrivateKey)
		if !ok {
			return fmt.Errorf("failed to read the RSA key %q, unknown format", keyID)
		}

		s.keys[keyID] = rsaKey
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func loadTLSCredentials(config Config) (*tls.Config, error) {

	serverCert, err := tls.LoadX509KeyPair(
		config.CertPath,
		config.KeyPath,
	)
	if err != nil {
		return nil, fmt.Errorf("could not load the server key pair: %v", err)
	}

	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(config.RootCAPath)
	if err != nil {
		return nil, fmt.Errorf("could not read the root CA certificate: %v", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, fmt.Errorf("failed to append ca certs")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
	}

	return tlsConfig, nil
}

////////////////////////////////////////////////////////////////////////////////

func run(configPath string) error {

	var config Config
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	tlsCreds, err := loadTLSCredentials(config)
	if err != nil {
		log.Fatalf("failed to load server TLS credentials: %v", err)
	}

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
	if err := rootKms.loadKeys(config); err != nil {
		return fmt.Errorf("failed to load keys: %v", err)
	}

	kms.RegisterSymmetricCryptoServiceServer(s, rootKms)

	return s.Serve(lis)
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configPath string
	var rootCmd = &cobra.Command{
		Use:   "fake-root-kms",
		Short: "Fake for RootKMS",
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
