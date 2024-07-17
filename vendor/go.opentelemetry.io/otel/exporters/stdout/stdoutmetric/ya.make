GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    encoder.go
    exporter.go
)

GO_XTEST_SRCS(
    example_test.go
    exporter_test.go
)

END()

RECURSE(
    gotest
)
