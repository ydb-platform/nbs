GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    exporter.go
    recorder.go
    span.go
)

GO_TEST_SRCS(
    exporter_test.go
    recorder_test.go
)

END()

RECURSE(
    gotest
)
