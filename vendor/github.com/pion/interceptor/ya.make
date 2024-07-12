GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    attributes.go
    chain.go
    errors.go
    interceptor.go
    noop.go
    registry.go
    streaminfo.go
)

GO_TEST_SRCS(
    attributes_test.go
    errors_test.go
)

END()

RECURSE(
    examples
    gotest
    internal
    pkg
)
