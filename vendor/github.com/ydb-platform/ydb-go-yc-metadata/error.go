package yc

import "fmt"

// createTokenError contains reason of token creation failure.
type createTokenError struct {
	Cause  error
	Reason string
}

// Error implements error interface.
func (e *createTokenError) Error() string {
	return fmt.Sprintf("metadata: create token error: %s", e.Reason)
}

func (e *createTokenError) Unwrap() error {
	return e.Cause
}
