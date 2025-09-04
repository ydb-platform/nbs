GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    context.go
    headers.go
    incoming.go
    meta.go
    trace_id.go
)

GO_TEST_SRCS(
    context_test.go
    trace_id_test.go
)

END()

RECURSE(
    gotest
    test
)
