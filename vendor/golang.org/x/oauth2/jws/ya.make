GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    jws.go
)

GO_TEST_SRCS(jws_test.go)

END()

RECURSE(
    gotest
)
