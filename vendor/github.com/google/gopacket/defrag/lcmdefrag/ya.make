GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    lcmdefrag.go
)

GO_TEST_SRCS(lcmdefrag_test.go)

END()

RECURSE(
    gotest
)
