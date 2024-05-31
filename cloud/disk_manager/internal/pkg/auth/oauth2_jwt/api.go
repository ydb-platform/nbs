package oauth2_jwt

import (
	"net/url"
	"strings"
)

type TokenResponse struct {
	AccessToken     string `json:"access_token,omitempty"`
	IssuedTokenType string `json:"issued_token_type,omitempty"`
	TokenType       string `json:"token_type,omitempty"`
	ExpiresIn       int64  `json:"expires_in,omitempty"`
}

func tokenRequest(token string) *strings.Reader {
	values := url.Values{
		"grant_type":           []string{"urn:ietf:params:oauth:grant-type:token-exchange"},
		"requested_token_type": []string{"urn:ietf:params:oauth:token-type:access_token"},
		"subject_token":        []string{token},
		"subject_token_type":   []string{"urn:ietf:params:oauth:token-type:jwt"},
	}

	return strings.NewReader(values.Encode())
}
