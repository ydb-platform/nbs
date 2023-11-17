package ssh

import "context"

////////////////////////////////////////////////////////////////////////////////

type HostResult struct {
	Output []string
	Err    error
}

type SSHIface interface {
	Run(ctx context.Context, cmd []string, targets []string) map[string]HostResult
}
