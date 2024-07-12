GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    reader.go
)

GO_TEST_SRCS(reader_test.go)

END()

RECURSE(
    gotest
)
