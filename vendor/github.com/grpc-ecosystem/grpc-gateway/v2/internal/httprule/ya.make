GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    compile.go
    parse.go
    types.go
)

GO_TEST_SRCS(
    compile_test.go
    parse_test.go
    types_test.go
)

END()

RECURSE(
    gotest
)
