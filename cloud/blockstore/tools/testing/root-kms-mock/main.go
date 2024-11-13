package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/kms"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type MockConfig struct {
	Port           uint32   `json:"port"`
	Keys           []string `json:"keys"`
	CertsOutputDir string   `json:"certs_output_dir"`
}

////////////////////////////////////////////////////////////////////////////////

type rootKmsService struct {
	kms.UnimplementedSymmetricCryptoServiceServer
	keys map[string]*rsa.PrivateKey
	mtx  sync.RWMutex
}

func (s *rootKmsService) generateKeys(keys []string) error {

	for _, keyID := range keys {
		log.Printf("generating RSA-4096 for %q", keyID)
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

////////////////////////////////////////////////////////////////////////////////

func generateCA(outputDir string) (
	*rsa.PrivateKey,
	*x509.Certificate,
	[]byte,
	error,
) {

	log.Println("generating CA")

	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate CA key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Root KMS CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(
		rand.Reader,
		caTemplate,
		caTemplate,
		&caKey.PublicKey,
		caKey,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create CA cert: %w", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	})

	caPath := filepath.Join(outputDir, "ca.crt")
	log.Printf("store CA to %s\n", caPath)

	if err := os.WriteFile(caPath, caCertPEM, 0644); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to save CA cert: %w", err)
	}

	return caKey, caTemplate, caCertPEM, nil
}

func generateServerTLSCredentials(
	caKey *rsa.PrivateKey,
	caTemplate *x509.Certificate,
	caCert []byte,
) (*tls.Config, error) {

	log.Println("generating server credentials")

	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Root KMS server"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"}, // fqdn?
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	certDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		caTemplate,
		&key.PublicKey,
		caKey,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert/key: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA cert")
	}

	tlsConfig := &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
	}

	return tlsConfig, nil
}

func generateClientCerts(
	outputDir string,
	caKey *rsa.PrivateKey,
	caTemplate *x509.Certificate,
) error {
	log.Println("generating client credentials")

	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Root KMS client"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		caTemplate,
		&key.PublicKey,
		caKey,
	)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	certPath := filepath.Join(outputDir, "client.crt")
	keyPath := filepath.Join(outputDir, "client.key")

	log.Printf("store the client certificate to %s\n", certPath)

	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("failed to save client cert: %w", err)
	}

	log.Printf("store the client key to %s\n", keyPath)

	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("failed to save client key: %w", err)
	}

	return nil
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

	caKey, caTemplate, caCert, err := generateCA(config.CertsOutputDir)
	if err != nil {
		log.Fatalf("failed to generate CA: %v", err)
	}

	tlsCreds, err := generateServerTLSCredentials(caKey, caTemplate, caCert)
	if err != nil {
		log.Fatalf("failed to generate server TLS credentials: %v", err)
	}

	err = generateClientCerts(config.CertsOutputDir, caKey, caTemplate)
	if err != nil {
		log.Fatalf("failed to generate client TLS credentials: %v", err)
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
