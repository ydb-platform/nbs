GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    nop_exporter.go
)

GO_TEST_SRCS(nop_exporter_test.go)

END()

RECURSE(
    gotest
)
