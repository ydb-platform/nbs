GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    collector.go
)

GO_TEST_SRCS(client_test.go)

END()

RECURSE(
    gotest
)
