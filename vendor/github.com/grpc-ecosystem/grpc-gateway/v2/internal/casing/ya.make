GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    camel.go
)

GO_TEST_SRCS(camel_test.go)

END()

RECURSE(
    gotest
)
