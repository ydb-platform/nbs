package oauth2_jwt

import (
	"crypto/rsa"
	"github.com/golang-jwt/jwt/v4"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"net/url"
	"os"
	"time"
)

const jwtTokenTTL = 10 * time.Minute

type jwtGenerator struct {
	issuer     string
	privateKey *rsa.PrivateKey
	subject    string
	audience   []string
	ttl        time.Duration
	id         string
	now        nowFunc
}

func (j *jwtGenerator) safeGenerateAndSignToken() (string, error) {
	now := j.now()
	token := jwt.NewWithClaims(
		jwt.SigningMethodRS256,
		jwt.RegisteredClaims{
			Issuer:    j.issuer,
			Subject:   j.subject,
			Audience:  j.audience,
			ExpiresAt: jwt.NewNumericDate(now.Add(j.ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	)
	token.Header["kid"] = j.id
	line, err := token.SignedString(j.privateKey)

	if err != nil {
		return "", err
	}

	return line, nil
}

func (j *jwtGenerator) generateAndSignToken() string {
	result, err := j.safeGenerateAndSignToken()

	if err != nil {
		// Not being able to sign a token is considered a
		// case of invalid configuration, it's ok to panic
		panic(err)
	}

	return result
}

func newJwtGenerator(config *config.AuthConfig, now nowFunc) (*jwtGenerator, error) {
	if config == nil {
		return nil, errors.NewNonRetriableErrorf("Authorization config is empty")
	}

	certificatePath := config.GetCertFile()
	if certificatePath == "" {
		return nil, errors.NewNonRetriableErrorf("No cert file in the config")
	}

	privateKeyData, err := os.ReadFile(certificatePath)
	if err != nil {
		return nil, err
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyData)

	serviceAccount := config.GetServiceAccount()
	if serviceAccount == nil {
		return nil, errors.NewNonRetriableErrorf("No service account config")
	}

	serviceAccountId := serviceAccount.GetId()
	if serviceAccountId == "" {
		return nil, errors.NewNonRetriableErrorf("Service account id string is empty")
	}

	parsedUrl, err := url.Parse(config.GetMetadataUrl())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	generator := &jwtGenerator{
		issuer:     serviceAccountId,
		privateKey: privateKey,
		subject:    serviceAccountId,
		audience:   []string{parsedUrl.Hostname()},
		ttl:        jwtTokenTTL,
		id:         serviceAccount.GetKeyId(),
		now:        now,
	}

	_, err = generator.safeGenerateAndSignToken()
	if err != nil {
		return nil, errors.NewNonRetriableErrorf("Error while checking token signing %v", err)
	}
	return generator, nil
}
