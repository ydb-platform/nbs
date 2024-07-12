GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    authhandler.go
)

GO_TEST_SRCS(authhandler_test.go)

END()

RECURSE(
    gotest
)
