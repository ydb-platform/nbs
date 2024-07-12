GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    context.go
    lexer.go
    literalfield_set.go
    parser.go
    string_util.go
)

GO_TEST_SRCS(
    lexer_test.go
    parser_test.go
)

END()

RECURSE(
    gotest
)
