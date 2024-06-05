package oauth2_jwt

import (
	"github.com/stretchr/testify/require"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"testing"
	"time"
)

func ptr[T any](v T) *T {
	return &v
}

func TestJWTSigning(t *testing.T) {
	expected := "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5IiwidHlwIjoiSldUIn0." +
		"eyJpc3MiOiJ0ZXN0LXNhIiwic3ViIjoidGVzdC1zYSIsImF1ZCI6WyJleGFtcGxlLmRp" +
		"c2stbWFuYWdlci5zdmMubG9jYWwiXSwiZXhwIjoxNzE3NDE3Mjk3LCJuYmYiOjE3MTc0" +
		"MTY2OTcsImlhdCI6MTcxNzQxNjY5N30.IcqEgOe3GQD4B4oO4V7hAu-n8MxJgmA_nT2g" +
		"SBcCjLlvfdyHSwe5VszwV1llpVkC50o0aV1k8_fa2bJgfzODKGvTBfawsiEhbEWoX-am" +
		"C7YDwEfq2zat3UvXRInhpYm8XhA1mK8De4uN3kmruHbLJf1-hfiFVYca8kWaIfC6pCiA" +
		"14MNFSGT00NAl7OgysL94tPaCXfsb81H-KMTNx0rPnyQdlarDRbwnwiogI0oRivLEXuS" +
		"3eW4wrVsB9VFvccVjN79okbGl4JgCn126dU05pt4UdXnJHjD72SEqLEnhtP3ACjKivLg" +
		"5InvMpLnU8q0jSO3swNhAOBaie-VCFuVB4BwqryLrDcaBAu05KklW6VxY69Z3DL7-O68" +
		"V57Sy84BDEigLJLLYnJVfXZKP2Byn0TbG1CW26aoJYDd8MJ3zvXoaGNfYo09YxmDamo6" +
		"9BFDLEK6zOQqoW2TgvRRDRVi9nuUsnWbBPx4qvqIoPJF6LxK0fzFmLLLgU5lpQESXVZ5" +
		"JWdBy1wlVdCW8SVw5bJdVRZFG0xaRn2U6DQFPfpyYWb0Ak1rv45gMmh09HaRPf8rwOzl" +
		"jnfA9wC4kNaqDXwSQgpd3iQK71oJH62JNzEiGHDY5k88gUDxDan3n2YnavITAbqFFf2e" +
		"KORpLctN8Z8GAcSlD_JFKwxz6X2pGs06B5g"
	tokenSigningTime := time.Unix(1717416697, 0)
	url := "https://example.disk-manager.svc.local:443/oauth2/token/exchange"
	generator, err := newJwtGenerator(&auth_config.AuthConfig{
		DisableAuthorization: ptr(false),
		MetadataUrl:          ptr(url),
		CertFile:             ptr("test_private.pem"),
		ServiceAccount: &auth_config.ServiceAccount{
			Id:    ptr("test-sa"),
			KeyId: ptr("test-key"),
		},
	},
		func() time.Time {
			return tokenSigningTime
		},
	)

	require.NoError(t, err)
	require.Equal(t, expected, generator.generateAndSignToken())
}
