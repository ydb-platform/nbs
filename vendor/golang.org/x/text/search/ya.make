GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    index.go
    pattern.go
    search.go
    tables.go
)

GO_TEST_SRCS(pattern_test.go)

END()

RECURSE(
    gotest
)
