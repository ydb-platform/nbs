GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    position.go
    token.go
)

GO_TEST_SRCS(token_test.go)

END()

RECURSE(
    gotest
)
