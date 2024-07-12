GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    override.go
    unicode.go
)

GO_TEST_SRCS(unicode_test.go)

END()

RECURSE(
    gotest
    utf32
)
