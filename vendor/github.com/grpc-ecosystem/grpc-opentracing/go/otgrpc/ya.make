GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    client.go
    errors.go
    options.go
    package.go
    server.go
    shared.go
)

GO_TEST_SRCS(errors_test.go)

END()

RECURSE(
    gotest
    test
)
