GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client_interceptors.go
    context.go
    doc.go
    grpclogger.go
    options.go
    payload_interceptors.go
    server_interceptors.go
)

GO_TEST_SRCS(
    grpclogger_test.go
    options_test.go
)

GO_XTEST_SRCS(
    client_interceptors_test.go
    examples_test.go
    payload_interceptors_test.go
    server_interceptors_test.go
    settable_test.go
    shared_test.go
)

END()

RECURSE(
    ctxzap
    # gotest
)
