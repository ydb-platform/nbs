GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    version.go
)

GO_XTEST_SRCS(version_test.go)

END()

RECURSE(
    gotest
    instrumentation
    internal
    resource
    trace
)
