GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    config.go
    doc.go
    exporter.go
)

GO_TEST_SRCS(
    client_test.go
    exporter_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    internal
)
