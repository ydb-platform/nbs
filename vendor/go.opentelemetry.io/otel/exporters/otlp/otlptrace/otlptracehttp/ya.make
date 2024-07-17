GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client.go
    doc.go
    exporter.go
    options.go
)

GO_XTEST_SRCS(
    certificate_test.go
    client_test.go
    example_test.go
    mock_collector_test.go
)

END()

RECURSE(
    gotest
    internal
)
