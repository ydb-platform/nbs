GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    bytediff.go
)

GO_TEST_SRCS(bytediff_test.go)

END()

RECURSE(
    gotest
)
