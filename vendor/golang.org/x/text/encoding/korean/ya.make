GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    euckr.go
    tables.go
)

GO_TEST_SRCS(all_test.go)

END()

RECURSE(
    gotest
)
