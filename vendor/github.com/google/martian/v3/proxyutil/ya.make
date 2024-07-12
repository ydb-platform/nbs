GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    header.go
    proxyutil.go
)

GO_TEST_SRCS(
    header_test.go
    proxyutil_test.go
)

END()

RECURSE(
    gotest
)
