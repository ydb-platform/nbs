GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    static_file_server.go
)

GO_TEST_SRCS(static_file_server_test.go)

END()

RECURSE(
    gotest
)
