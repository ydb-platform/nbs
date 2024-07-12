GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    work.go
)

GO_TEST_SRCS(work_test.go)

END()

RECURSE(
    gotest
)
