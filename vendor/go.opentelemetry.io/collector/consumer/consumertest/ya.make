GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    consumer.go
    doc.go
    err.go
    nop.go
    sink.go
)

GO_TEST_SRCS(
    err_test.go
    nop_test.go
    sink_test.go
)

END()

RECURSE(
    gotest
)
