GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    jose.go
    jwks.go
    oidc.go
    verify.go
)

GO_TEST_SRCS(
    jwks_test.go
    oidc_test.go
    verify_test.go
)

END()

RECURSE(
    gotest
)
