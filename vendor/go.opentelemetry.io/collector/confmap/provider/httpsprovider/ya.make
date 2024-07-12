GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    provider.go
)

GO_TEST_SRCS(provider_test.go)

END()

RECURSE(
    gotest
)
