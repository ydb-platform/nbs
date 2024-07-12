GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    htmlindex.go
    map.go
    tables.go
)

GO_TEST_SRCS(htmlindex_test.go)

END()

RECURSE(
    gotest
)
