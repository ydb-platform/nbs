GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    ast.go
    ast_attribute.go
    ast_block.go
    ast_body.go
    ast_expression.go
    doc.go
    format.go
    generate.go
    native_node_sorter.go
    node.go
    parser.go
    public.go
    tokens.go
)

GO_TEST_SRCS(
    ast_block_test.go
    ast_body_test.go
    ast_test.go
    format_test.go
    generate_test.go
    parser_test.go
    round_trip_test.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    fuzz
    gotest
)
