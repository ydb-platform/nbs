GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    exporter.go
)

GO_TEST_SRCS(
    benchmark_test.go
    config_test.go
    exporter_test.go
)

END()

RECURSE(
    gotest
)
