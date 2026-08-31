package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	tokenexchange_mock "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/tokenexchange"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewFederatedCredentials(t *testing.T) {
	const (
		subjectTokenType = "urn:example:params:oauth:token-type:subject"
		actorTokenType   = "urn:ietf:params:oauth:token-type:jwt"
	)

	mock := tokenexchange_mock.New("iam-token")
	server := httptest.NewTLSServer(mock)
	defer server.Close()

	defaultTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	defer func() {
		http.DefaultTransport = defaultTransport
	}()

	credentials, err := NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String(server.URL),
		SubjectToken:          tokenValue("subject-token", subjectTokenType),
		ActorToken: tokenFile(
			writeTokenFile(t, "actor-token\n"),
			actorTokenType,
		),
	})
	require.NoError(t, err)

	token, err := credentials.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer iam-token", token)

	token, err = credentials.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer iam-token", token)
	require.Equal(t, []tokenexchange_mock.Request{{
		GrantType:          tokenexchange_mock.TokenExchangeGrantType,
		RequestedTokenType: tokenexchange_mock.AccessTokenType,
		SubjectToken:       "subject-token",
		SubjectTokenType:   subjectTokenType,
		ActorToken:         "actor-token",
		ActorTokenType:     actorTokenType,
	}}, mock.Requests())
}

func TestTokenSourceRereadsTokenFile(t *testing.T) {
	tokenPath := writeTokenFile(t, "token-1\n")
	source, err := newTokenSource(
		"subject",
		tokenFile(tokenPath, "token-type"),
	)
	require.NoError(t, err)

	token, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, "token-1", token.Token)

	require.NoError(t, os.WriteFile(tokenPath, []byte("token-2\n"), 0600))
	token, err = source.Token()
	require.NoError(t, err)
	require.Equal(t, "token-2", token.Token)
}

func TestFederatedCredentialsValidation(t *testing.T) {
	_, err := NewFederatedCredentials(nil)
	require.ErrorContains(t, err, "federated credentials config is missing")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("grpcs://tokens.example.com"),
		SubjectToken:          tokenValue("token", "token-type"),
	})
	require.ErrorContains(t, err, "invalid HTTPS token exchange endpoint")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("http://sts.example.com"),
		SubjectToken:          tokenValue("token", "token-type"),
	})
	require.ErrorContains(t, err, "invalid HTTPS token exchange endpoint")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
	})
	require.ErrorContains(t, err, "subject token is missing")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
		SubjectToken: &TypedToken{
			TokenType: proto.String("token-type"),
		},
	})
	require.ErrorContains(t, err, "subject token source is missing")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
		SubjectToken:          tokenValue(" ", "token-type"),
	})
	require.ErrorContains(t, err, "subject token source is empty")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
		SubjectToken:          tokenFile(" ", "token-type"),
	})
	require.ErrorContains(t, err, "subject token source is empty")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
		SubjectToken:          tokenValue("subject-token", "token-type"),
		ActorToken:            tokenValue(" ", "token-type"),
	})
	require.ErrorContains(t, err, "actor token source is empty")

	_, err = NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("https://sts.example.com"),
		SubjectToken:          tokenValue("subject-token", "token-type"),
		ActorToken:            tokenFile(" ", "token-type"),
	})
	require.ErrorContains(t, err, "actor token source is empty")
}

func tokenValue(value string, tokenType string) *TypedToken {
	return &TypedToken{
		TokenType: proto.String(tokenType),
		Source:    &auth_config.TypedToken_Value{Value: value},
	}
}

func tokenFile(file string, tokenType string) *TypedToken {
	return &TypedToken{
		TokenType: proto.String(tokenType),
		Source:    &auth_config.TypedToken_File{File: file},
	}
}

func writeTokenFile(t *testing.T, token string) string {
	file, err := os.CreateTemp(t.TempDir(), "federated-token-")
	require.NoError(t, err)
	defer file.Close()

	_, err = file.WriteString(token)
	require.NoError(t, err)
	return file.Name()
}
