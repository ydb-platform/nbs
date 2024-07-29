GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    config.go
    doc.go
    trace.go
)

GO_XTEST_SRCS(
    example_test.go
    trace_test.go
)

END()

RECURSE(
    gotest
)
