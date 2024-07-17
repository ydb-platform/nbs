GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    http.go
    net.go
)

GO_TEST_SRCS(
    http_test.go
    net_test.go
)

END()

RECURSE(
    gotest
)
