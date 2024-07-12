GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    client_interceptors.go
    doc.go
    options.go
    payload_interceptors.go
    server_interceptors.go
)

GO_XTEST_SRCS(
    client_interceptors_test.go
    examples_test.go
    payload_interceptors_test.go
    server_interceptors_test.go
    shared_test.go
)

END()

RECURSE(
    ctxkit
    gotest
)
