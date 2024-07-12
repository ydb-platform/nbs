GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    configauth.go
)

GO_TEST_SRCS(configauth_test.go)

END()

RECURSE(
    gotest
)
