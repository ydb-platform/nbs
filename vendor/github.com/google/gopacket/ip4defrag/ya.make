GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    defrag.go
)

GO_TEST_SRCS(defrag_test.go)

END()

RECURSE(
    gotest
)
