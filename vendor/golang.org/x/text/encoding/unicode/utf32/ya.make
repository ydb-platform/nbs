GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    utf32.go
)

GO_TEST_SRCS(utf32_test.go)

END()

RECURSE(
    gotest
)
