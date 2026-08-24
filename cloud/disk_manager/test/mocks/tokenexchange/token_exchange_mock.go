package tokenexchange

import (
	"encoding/json"
	"net/http"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////

const (
	TokenExchangeGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"
	AccessTokenType        = "urn:ietf:params:oauth:token-type:access_token"
)

type Request struct {
	GrantType          string
	RequestedTokenType string
	SubjectToken       string
	SubjectTokenType   string
	ActorToken         string
	ActorTokenType     string
	Audience           string
}

type Mock struct {
	accessToken string

	mutex    sync.Mutex
	requests []Request
}

func New(accessToken string) *Mock {
	return &Mock{accessToken: accessToken}
}

func (m *Mock) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "only POST is supported", http.StatusMethodNotAllowed)
		return
	}

	if err := request.ParseForm(); err != nil {
		http.Error(writer, "failed to parse form", http.StatusBadRequest)
		return
	}

	tokenExchangeRequest := Request{
		GrantType:          request.Form.Get("grant_type"),
		RequestedTokenType: request.Form.Get("requested_token_type"),
		SubjectToken:       request.Form.Get("subject_token"),
		SubjectTokenType:   request.Form.Get("subject_token_type"),
		ActorToken:         request.Form.Get("actor_token"),
		ActorTokenType:     request.Form.Get("actor_token_type"),
		Audience:           request.Form.Get("audience"),
	}

	if tokenExchangeRequest.GrantType != TokenExchangeGrantType ||
		tokenExchangeRequest.RequestedTokenType != AccessTokenType ||
		tokenExchangeRequest.SubjectToken == "" ||
		tokenExchangeRequest.SubjectTokenType == "" ||
		(tokenExchangeRequest.ActorToken == "") !=
			(tokenExchangeRequest.ActorTokenType == "") {

		http.Error(writer, "invalid token exchange request", http.StatusBadRequest)
		return
	}

	m.mutex.Lock()
	m.requests = append(m.requests, tokenExchangeRequest)
	m.mutex.Unlock()

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(map[string]interface{}{
		"access_token":      m.accessToken,
		"issued_token_type": AccessTokenType,
		"token_type":        "Bearer",
		"expires_in":        3600,
	}); err != nil {
		http.Error(writer, "failed to encode response", http.StatusInternalServerError)
	}
}

func (m *Mock) Requests() []Request {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return append([]Request(nil), m.requests...)
}
