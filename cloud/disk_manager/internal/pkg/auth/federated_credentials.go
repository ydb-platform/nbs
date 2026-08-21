package auth

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type FederatedCredentials = auth_config.FederatedCredentials

type TypedToken = auth_config.TypedToken

////////////////////////////////////////////////////////////////////////////////

type tokenSource struct {
	value     string
	file      string
	tokenType string
}

func (s *tokenSource) Token() (credentials.Token, error) {
	value := s.value
	if s.file != "" {
		token, err := os.ReadFile(s.file)
		if err != nil {
			return credentials.Token{}, fmt.Errorf("read token file: %w", err)
		}
		value = string(token)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return credentials.Token{}, fmt.Errorf("token is empty")
	}

	return credentials.Token{
		Token:     value,
		TokenType: s.tokenType,
	}, nil
}

func (s *tokenSource) String() string {
	if s.file != "" {
		return fmt.Sprintf(
			"FileTokenSource{File:%q,Type:%s}",
			s.file,
			s.tokenType,
		)
	}

	return fmt.Sprintf("FixedTokenSource{Type:%s}", s.tokenType)
}

func newTokenSource(
	name string,
	config *TypedToken,
) (*tokenSource, error) {

	if config == nil {
		return nil, fmt.Errorf("%s token is missing", name)
	}

	tokenType := strings.TrimSpace(config.GetTokenType())
	if tokenType == "" {
		return nil, fmt.Errorf("%s token type is empty", name)
	}

	if config.GetSource() == nil {
		return nil, fmt.Errorf("%s token source is missing", name)
	}

	value := strings.TrimSpace(config.GetValue())
	file := strings.TrimSpace(config.GetFile())

	return &tokenSource{
		value:     value,
		file:      file,
		tokenType: tokenType,
	}, nil
}

func NewFederatedCredentials(
	config *FederatedCredentials,
) (Credentials, error) {

	tokenExchangeEndpoint := strings.TrimSpace(
		config.GetTokenExchangeEndpoint(),
	)
	endpoint, err := url.ParseRequestURI(tokenExchangeEndpoint)
	if err != nil || endpoint.Host == "" ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") {

		return nil, fmt.Errorf(
			"invalid HTTP token exchange endpoint %q",
			tokenExchangeEndpoint,
		)
	}

	subjectToken, err := newTokenSource(
		"subject",
		config.GetSubjectToken(),
	)
	if err != nil {
		return nil, err
	}

	options := []credentials.Oauth2TokenExchangeCredentialsOption{
		credentials.WithTokenEndpoint(tokenExchangeEndpoint),
		credentials.WithSubjectToken(subjectToken),
	}

	if actorTokenConfig := config.GetActorToken(); actorTokenConfig != nil {
		actorToken, err := newTokenSource(
			"actor",
			actorTokenConfig,
		)
		if err != nil {
			return nil, err
		}
		options = append(options, credentials.WithActorToken(actorToken))
	}

	result, err := credentials.NewOauth2TokenExchangeCredentials(options...)
	if err != nil {
		return nil, fmt.Errorf("create OAuth2 token exchange credentials: %w", err)
	}

	return result, nil
}
