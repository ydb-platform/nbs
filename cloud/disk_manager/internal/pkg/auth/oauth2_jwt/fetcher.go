package oauth2_jwt

import (
	"context"
	"encoding/json"
	"fmt"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"net/http"
)

type TokenFetcher interface {
	Fetch(ctx context.Context) (Token, error)
}

type InvalidTokenResponseError struct {
	StatusCode int
	Body       string
}

func (err InvalidTokenResponseError) Error() string {
	return fmt.Sprintf(
		"Token request failed with status: %d and body: %s",
		err.StatusCode,
		err.Body,
	)
}

type plainTokenFetcher struct {
	tokenUrl     string
	jwtGenerator *jwtGenerator
	now          nowFunc
}

func (fetcher plainTokenFetcher) Fetch(ctx context.Context) (Token, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fetcher.tokenUrl,
		tokenRequest(fetcher.jwtGenerator.generateAndSignToken()),
	)

	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)

	if err != nil {
		return nil, err
	}

	body := make([]byte, response.ContentLength)
	_, err = response.Body.Read(body)

	if response.StatusCode != 200 {
		return nil, InvalidTokenResponseError{
			StatusCode: response.StatusCode,
			Body:       string(body),
		}
	}

	tokenResponse := &TokenResponse{}
	err = json.Unmarshal(body, tokenResponse)
	if err != nil {
		return nil, err
	}

	return newBearerToken(
		tokenResponse.AccessToken,
		tokenResponse.IssuedTokenType,
		tokenResponse.TokenType,
		fetcher.now,
		tokenResponse.ExpiresIn,
	), nil
}

func newOauth2JWTTokenFetcher(
	config *auth_config.AuthConfig,
	now nowFunc,
) (TokenFetcher, error) {
	if config.GetMetadataUrl() == "" {
		return nil, errors.NewNonRetriableErrorf("No metadata url")
	}

	jwtGenerator, err := newJwtGenerator(config, now)
	if err != nil {
		return nil, err
	}

	return &plainTokenFetcher{
		tokenUrl:     config.GetMetadataUrl(),
		jwtGenerator: jwtGenerator,
		now:          now,
	}, nil
}
