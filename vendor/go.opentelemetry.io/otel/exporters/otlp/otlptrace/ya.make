GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    clients.go
    doc.go
    exporter.go
    version.go
)

GO_XTEST_SRCS(
    exporter_test.go
    version_test.go
)

END()

RECURSE(
    gotest
    internal
)
