GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
    headers.go
    incoming.go
    meta.go
    trace_id.go
)

GO_TEST_SRCS(trace_id_test.go)

END()

RECURSE(
    gotest
    test
)
