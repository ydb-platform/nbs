GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    charmap.go
    tables.go
)

GO_TEST_SRCS(charmap_test.go)

END()

RECURSE(
    gotest
)
