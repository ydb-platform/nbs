GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    doc.go
    server.go
)

GO_TEST_SRCS(
    client_test.go
    server_test.go
)

END()

RECURSE(
    authtest
    gotest
)
