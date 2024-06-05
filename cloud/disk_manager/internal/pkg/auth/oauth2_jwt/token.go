package oauth2_jwt

import "time"

////////////////////////////////////////////////////////////////////////////////

type nowFunc func() time.Time

type Token interface {
	Expired() bool
	NeedToUpdate() bool
	Token() string
}

type BearerToken struct {
	token           string
	issuedTokenType string
	tokenType       string
	obtainedAt      time.Time
	now             nowFunc
	expiresIn       time.Duration
}

func (a BearerToken) NeedToUpdate() bool {
	return a.now().Sub(a.obtainedAt) > a.expiresIn/2
}

func (a BearerToken) Expired() bool {
	return a.now().Sub(a.obtainedAt) > a.expiresIn
}

func (a BearerToken) Token() string {
	return a.token
}

func newBearerToken(
	authToken string,
	issuedTokenType string,
	tokenType string,
	now nowFunc,
	expiresIn int64,
) Token {
	return &BearerToken{
		token:           authToken,
		issuedTokenType: issuedTokenType,
		tokenType:       tokenType,
		obtainedAt:      now(),
		now:             now,
		expiresIn:       time.Second * time.Duration(expiresIn),
	}
}
