GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    tables.go
)

GO_TEST_SRCS(
    data_test.go
    gen_test.go
)

END()

RECURSE(
    gotest
)
