package trace

import (
	"context"
	"time"
)

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

//gtrace:gen
//gtrace:set Shortcut
type Trace struct {
	OnRefreshToken func(RefreshTokenStartInfo) func(RefreshTokenDoneInfo)
	OnGetToken     func(GetTokenStartInfo) func(GetTokenDoneInfo)
}

type (
	RefreshTokenStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	RefreshTokenDoneInfo struct {
		Token     string
		ExpiresIn time.Duration
		Error     error
	}
	GetTokenStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	GetTokenDoneInfo struct {
		Token string
		Error error
	}
)
