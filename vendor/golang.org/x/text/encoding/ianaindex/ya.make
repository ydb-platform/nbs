GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    ascii.go
    ianaindex.go
    tables.go
)

GO_TEST_SRCS(
    ascii_test.go
    ianaindex_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
