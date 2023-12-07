package auth

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"

	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	metadata "github.com/ydb-platform/ydb-go-yc-metadata"
	"github.com/ydb-platform/ydb-go-yc-metadata/trace"
)

////////////////////////////////////////////////////////////////////////////////

type Credentials = auth.Credentials

func NewCredentials(
	ctx context.Context,
	config *auth_config.AuthConfig,
) Credentials {

	if len(config.GetMetadataUrl()) == 0 {
		return nil
	}

	return &credentialsWrapper{
		impl: metadata.NewInstanceServiceAccount(
			metadata.WithURL(config.GetMetadataUrl()),
			metadata.WithTrace(trace.Trace{
				OnRefreshToken: func(info trace.RefreshTokenStartInfo) func(trace.RefreshTokenDoneInfo) {
					return func(info trace.RefreshTokenDoneInfo) {
						if info.Error == nil {
							logging.Info(
								ctx,
								"token refresh done (token: %s, expiresIn: %v)",
								maskToken(info.Token),
								info.ExpiresIn,
							)
						} else {
							logging.Error(ctx, "token refresh fail: %v", info.Error)
						}
					}
				},
			}),
		),
	}
}

////////////////////////////////////////////////////////////////////////////////

type credentialsWrapper struct {
	impl Credentials
}

func (c *credentialsWrapper) Token(ctx context.Context) (string, error) {
	token, err := c.impl.Token(ctx)
	if err != nil {
		// Ignore token errors.
		return "", errors.NewRetriableError(err)
	}

	return token, nil
}

////////////////////////////////////////////////////////////////////////////////

func maskToken(token string) string {
	var mask bytes.Buffer
	if len(token) > 16 {
		mask.WriteString(token[:4])
		mask.WriteString("****")
		mask.WriteString(token[len(token)-4:])
	} else {
		mask.WriteString("****")
	}
	mask.WriteString(fmt.Sprintf(
		"(CRC-32c: %08X)",
		crc32.Checksum([]byte(token), crc32.IEEETable),
	))
	return mask.String()
}
