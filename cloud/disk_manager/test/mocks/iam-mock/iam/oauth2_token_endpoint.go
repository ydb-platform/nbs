package iam

import (
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func randomString(size int) string {

	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	runes := make([]rune, size)
	for i := range runes {
		runes[i] = rune(letters[rand.Intn(len(letters))])
	}

	return string(runes)
}

////////////////////////////////////////////////////////////////////////////////

type TokenResponse struct {
	AccessToken     string `json:"access_token,omitempty"`
	IssuedTokenType string `json:"issued_token_type,omitempty"`
	TokenType       string `json:"token_type,omitempty"`
	ExpiresIn       int64  `json:"expires_in,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////

type ErrorResponse struct {
	ErrorType        string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
}

func newInvalidRequest(message string) *ErrorResponse {
	return &ErrorResponse{
		ErrorType:        "invalid_request",
		ErrorDescription: message,
	}
}

func (e ErrorResponse) Write(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusBadRequest)
	err := json.NewEncoder(writer).Encode(e)
	log.Printf("Error while fetching token %v", err)
}

////////////////////////////////////////////////////////////////////////////////

type TokenRequest struct {
	grantType          string
	requestedTokenType string
	subjectToken       string
	subjectTokenType   string
}

func (t TokenRequest) validate() *ErrorResponse {
	if t.grantType != "urn:ietf:params:oauth:grant-type:token-exchange" {
		return newInvalidRequest(fmt.Sprintf("unsupported grant type %s", t.grantType))
	}

	if t.requestedTokenType != "urn:ietf:params:oauth:token-type:access_token" {
		return newInvalidRequest(fmt.Sprintf("unsupported access token type %s", t.requestedTokenType))
	}

	if t.subjectTokenType != "urn:ietf:params:oauth:token-type:jwt" {
		return newInvalidRequest(fmt.Sprintf("unsupported subject token type %s", t.subjectTokenType))
	}

	return nil
}

func parseTokenRequest(request *http.Request) (TokenRequest, *ErrorResponse) {

	if err := request.ParseForm(); err != nil {
		return TokenRequest{}, newInvalidRequest(err.Error())
	}

	var tokenRequest TokenRequest

	tokenRequest.grantType = request.Form.Get("grant_type")
	if tokenRequest.grantType == "" {
		return TokenRequest{}, newInvalidRequest("missing field grant_type")
	}

	tokenRequest.requestedTokenType = request.Form.Get("requested_token_type")
	if tokenRequest.requestedTokenType == "" {
		return TokenRequest{}, newInvalidRequest("missing field requested_token_type")
	}

	tokenRequest.subjectToken = request.Form.Get("subject_token")
	if tokenRequest.subjectToken == "" {
		return TokenRequest{}, newInvalidRequest("missing field subject_token")
	}

	tokenRequest.subjectTokenType = request.Form.Get("subject_token_type")
	if tokenRequest.subjectTokenType == "" {
		return TokenRequest{}, newInvalidRequest("missing field subject_token_type")
	}

	return tokenRequest, nil
}

////////////////////////////////////////////////////////////////////////////////

type IssuedTokenType struct {
	subject   string
	value     string
	issuedAt  time.Time
	expiresIn time.Duration
}

func (i IssuedTokenType) check() bool {
	return i.issuedAt.Add(i.expiresIn).Sub(time.Now()) > 0
}

////////////////////////////////////////////////////////////////////////////////

type CreateUserRequest struct {
	KeyId          string `json:"key_id,omitempty"`
	AccountId      string `json:"account_id,omitempty"`
	Audience       string `json:"audience,omitempty"`
	IssuedTokenTTL uint64 `json:"issued_token_ttl,omitempty"`
}

type CreateUserResponse struct {
	PrivateKey string `json:"private_key"`
}

type IssueTokenFunc func(tokenType IssuedTokenType)

////////////////////////////////////////////////////////////////////////////////

type jwtToken struct {
	issuer    string
	subject   string
	audience  string
	keyId     string
	tokenTTL  time.Duration
	publicKey *rsa.PublicKey
}

type Oauth2TokenService struct {
	onTokenIssue IssueTokenFunc
	mutex        sync.Mutex
	jwtTokens    map[string]jwtToken
	mux          *http.ServeMux
}

func NewOauth2TokenService(mux *http.ServeMux, onTokenIssue IssueTokenFunc) *Oauth2TokenService {
	service := &Oauth2TokenService{
		onTokenIssue: onTokenIssue,
		mutex:        sync.Mutex{},
		jwtTokens:    make(map[string]jwtToken),
		mux:          mux,
	}
	service.registerHandlers()

	return service
}

func (t *Oauth2TokenService) ObtainToken(tokenString string) (*TokenResponse, *ErrorResponse) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	var expectedToken jwtToken
	token, err := jwt.Parse(
		tokenString,
		func(token *jwt.Token) (interface{}, error) {
			kidInterface, ok := token.Header["kid"]
			if !ok {
				return nil, errors.New("no header in token")
			}

			kid, ok := kidInterface.(string)
			if !ok {
				return nil, errors.New("invalid key id type")
			}

			expectedToken, ok = t.jwtTokens[kid]
			if !ok {
				return nil, errors.New("unknown token")
			}

			return expectedToken.publicKey, nil
		},
	)
	if err != nil {
		return nil, newInvalidRequest(err.Error())
	}

	err = token.Claims.Valid()
	if err != nil {
		return nil, newInvalidRequest(err.Error())
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, newInvalidRequest("can't cast token claims to jwt map claims")
	}

	if !claims.VerifyExpiresAt(time.Now().Unix(), true) {
		return nil, newInvalidRequest("invalid token field ExpiresAt")
	}

	if !claims.VerifyIssuer(expectedToken.issuer, true) {
		return nil, newInvalidRequest("invalid token field Issuer")
	}

	if !claims.VerifyAudience(expectedToken.audience, true) {
		return nil, newInvalidRequest("invalid token field Audience")
	}

	if !claims.VerifyIssuedAt(time.Now().Unix(), true) {
		return nil, newInvalidRequest("invalid token field IssuedAt")
	}

	issuedToken := randomString(64)

	t.onTokenIssue(
		IssuedTokenType{
			value:     issuedToken,
			subject:   expectedToken.subject,
			issuedAt:  time.Now(),
			expiresIn: expectedToken.tokenTTL,
		},
	)

	return &TokenResponse{
		AccessToken:     issuedToken,
		IssuedTokenType: "urn:ietf:params:oauth:token-type:access_token",
		TokenType:       "bearer",
		ExpiresIn:       int64(expectedToken.tokenTTL.Seconds()),
	}, nil
}

func (t *Oauth2TokenService) CreateUser(request CreateUserRequest) (CreateUserResponse, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	key, err := rsa.GenerateKey(crypto_rand.Reader, 4096)
	if err != nil {
		return CreateUserResponse{}, err
	}

	publicKey := key.Public().(*rsa.PublicKey)
	privateKeyPem := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		},
	)

	t.jwtTokens[request.KeyId] = jwtToken{
		issuer:    request.AccountId,
		subject:   request.AccountId,
		audience:  request.Audience,
		keyId:     request.KeyId,
		tokenTTL:  time.Duration(request.IssuedTokenTTL) * time.Second,
		publicKey: publicKey,
	}

	return CreateUserResponse{
		PrivateKey: string(privateKeyPem),
	}, nil
}

func (t *Oauth2TokenService) registerHandlers() {
	t.mux.HandleFunc(
		"/oauth2/token/exchange",
		func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				http.Error(
					writer,
					"Invalid method",
					http.StatusMethodNotAllowed,
				)
				return
			}
			tokenRequest, err := parseTokenRequest(request)
			if err != nil {
				err.Write(writer)
				return
			}

			err = tokenRequest.validate()
			if err != nil {
				err.Write(writer)
				return
			}

			tokenResponse, err := t.ObtainToken(tokenRequest.subjectToken)
			if err != nil {
				err.Write(writer)
				return
			}

			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			jsonErr := json.NewEncoder(writer).Encode(tokenResponse)
			if err != nil {
				log.Printf("Error while marshaling json %v", jsonErr)
			}
		},
	)
	t.mux.HandleFunc(
		"/users/create",
		func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				http.Error(
					writer,
					"Invalid method",
					http.StatusMethodNotAllowed,
				)
				return
			}
			decoder := json.NewDecoder(request.Body)
			var createUserRequest CreateUserRequest
			err := decoder.Decode(&createUserRequest)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			response, err := t.CreateUser(createUserRequest)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			jsonErr := json.NewEncoder(writer).Encode(response)
			if jsonErr != nil {
				log.Printf("Error while marshaling json %v", jsonErr)
			}
		},
	)
}
