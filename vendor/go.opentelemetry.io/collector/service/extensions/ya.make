GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    extensions.go
)

GO_TEST_SRCS(extensions_test.go)

END()

RECURSE(
    gotest
)
