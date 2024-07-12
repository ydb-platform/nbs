GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    exporter.go
)

GO_TEST_SRCS(exporter_test.go)

END()

RECURSE(
    exporterhelper
    exportertest
    gotest
)
