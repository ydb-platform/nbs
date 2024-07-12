GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    diff.go
)

GO_TEST_SRCS(diff_test.go)

END()

RECURSE(
    gotest
)
