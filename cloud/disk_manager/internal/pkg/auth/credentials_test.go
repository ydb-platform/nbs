package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type credentialsMock struct {
	token string
	err   error
}

func (c *credentialsMock) Token(context.Context) (string, error) {
	return c.token, c.err
}

func TestNewCredentialsWithoutConfiguration(t *testing.T) {
	credentials, err := NewCredentials(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, credentials)
}

func TestNewCredentialsRejectsMixedFederatedConfiguration(t *testing.T) {
	credentials, err := NewCredentials(context.Background(), &AuthConfig{
		MetadataUrl: stringPointer("http://metadata.invalid"),
		FederatedCredentials: &FederatedCredentials{
			TokenExchangeEndpoint: stringPointer("https://sts.example.com"),
			SubjectToken:          tokenValue("token", "token-type"),
		},
	})
	require.ErrorContains(t, err, "cannot be combined")
	require.Nil(t, credentials)
}

func TestCredentialsWrapper(t *testing.T) {
	credentials := &credentialsWrapper{
		impl: &credentialsMock{token: "token"},
	}
	token, err := credentials.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "token", token)

	credentials = &credentialsWrapper{
		impl: &credentialsMock{err: fmt.Errorf("token error")},
	}
	_, err = credentials.Token(context.Background())
	require.ErrorContains(t, err, "token error")
	require.True(t, task_errors.CanRetry(err))
}

func stringPointer(value string) *string {
	return &value
}
