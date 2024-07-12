GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auth.go
    identity.go
    option.go
    scheme_id.go
)

GO_TEST_SRCS(option_test.go)

END()

RECURSE(
    bearer
    gotest
)
