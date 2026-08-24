package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewFederatedCredentials(t *testing.T) {
	const (
		subjectTokenType = "urn:example:params:oauth:token-type:subject"
		actorTokenType   = "urn:ietf:params:oauth:token-type:jwt"
	)

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		requestCount++
		require.Equal(t, http.MethodPost, request.Method)
		require.NoError(t, request.ParseForm())
		require.Equal(
			t,
			"urn:ietf:params:oauth:grant-type:token-exchange",
			request.Form.Get("grant_type"),
		)
		require.Equal(
			t,
			"urn:ietf:params:oauth:token-type:access_token",
			request.Form.Get("requested_token_type"),
		)
		require.Equal(t, "subject-token", request.Form.Get("subject_token"))
		require.Equal(
			t,
			subjectTokenType,
			request.Form.Get("subject_token_type"),
		)
		require.Equal(t, "actor-token", request.Form.Get("actor_token"))
		require.Equal(
			t,
			actorTokenType,
			request.Form.Get("actor_token_type"),
		)

		writer.Header().Set("Content-Type", "application/json")
		_, err := fmt.Fprint(writer, `{
			"access_token": "iam-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type": "Bearer",
			"expires_in": 3600
		}`)
		require.NoError(t, err)
	}))
	defer server.Close()

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
	require.Equal(t, 1, requestCount)
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
	_, err := NewFederatedCredentials(&FederatedCredentials{
		TokenExchangeEndpoint: proto.String("grpcs://tokens.example.com"),
		SubjectToken:          tokenValue("token", "token-type"),
	})
	require.ErrorContains(t, err, "invalid HTTP token exchange endpoint")

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
