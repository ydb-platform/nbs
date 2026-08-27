package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	tokenexchange_mock "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/tokenexchange"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewCredentialsWithRFC8693ServiceAccount(t *testing.T) {
	const (
		serviceAccountID = "service-account-id"
		keyID            = "key-id"
		audience         = "test-audience"
	)

	mock := tokenexchange_mock.New("iam-token")
	server := httptest.NewServer(mock)
	defer server.Close()

	privateKey, privateKeyFile := writePrivateKey(t)
	credentials, err := NewCredentials(context.Background(), &AuthConfig{
		MetadataUrl: proto.String(server.URL),
		ServiceAccount: &auth_config.ServiceAccount{
			Id:                   proto.String(serviceAccountID),
			KeyId:                proto.String(keyID),
			Audience:             proto.String(audience),
			TokenSigningCertFile: proto.String(privateKeyFile),
		},
	})
	require.NoError(t, err)

	token, err := credentials.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer iam-token", token)

	requests := mock.Requests()
	require.Len(t, requests, 1)
	request := requests[0]
	require.Equal(t, tokenexchange_mock.TokenExchangeGrantType, request.GrantType)
	require.Equal(t, tokenexchange_mock.AccessTokenType, request.RequestedTokenType)
	require.Equal(
		t,
		"urn:ietf:params:oauth:token-type:jwt",
		request.SubjectTokenType,
	)
	require.Equal(t, audience, request.Audience)
	require.Empty(t, request.ActorToken)
	require.Empty(t, request.ActorTokenType)

	jwtToken, err := jwt.Parse(request.SubjectToken, func(
		token *jwt.Token,
	) (interface{}, error) {

		require.Equal(t, jwt.SigningMethodRS256, token.Method)
		require.Equal(t, keyID, token.Header["kid"])
		return &privateKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, jwtToken.Valid)

	claims, ok := jwtToken.Claims.(jwt.MapClaims)
	require.True(t, ok)
	require.Equal(t, serviceAccountID, claims["iss"])
	require.Equal(t, serviceAccountID, claims["sub"])
	require.True(t, claims.VerifyAudience(audience, true))
}

func writePrivateKey(t *testing.T) (*rsa.PrivateKey, string) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	privateKeyFile := filepath.Join(t.TempDir(), "private-key.pem")
	require.NoError(t, os.WriteFile(
		privateKeyFile,
		pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		}),
		0600,
	))

	return privateKey, privateKeyFile
}
