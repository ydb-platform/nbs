GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    encode.go
)

GO_TEST_SRCS(encode_test.go)

END()

RECURSE(
    gotest
)
