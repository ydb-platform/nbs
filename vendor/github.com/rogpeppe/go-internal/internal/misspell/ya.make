GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    misspell.go
)

GO_TEST_SRCS(misspell_test.go)

END()

RECURSE(
    gotest
)
