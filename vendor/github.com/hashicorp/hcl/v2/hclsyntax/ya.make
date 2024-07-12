GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diagnostics.go
    didyoumean.go
    doc.go
    expression.go
    expression_ops.go
    expression_template.go
    expression_vars.go
    file.go
    generate.go
    keywords.go
    navigation.go
    node.go
    parser.go
    parser_template.go
    parser_traversal.go
    peeker.go
    public.go
    scan_string_lit.go
    scan_tokens.go
    structure.go
    structure_at_pos.go
    token.go
    token_type_string.go
    variables.go
    walk.go
)

GO_TEST_SRCS(
    didyoumean_test.go
    expression_static_test.go
    expression_template_test.go
    # expression_test.go
    expression_typeparams_test.go
    navigation_test.go
    parse_traversal_test.go
    parser_test.go
    peeker_test.go
    public_test.go
    scan_string_lit_test.go
    scan_tokens_test.go
    structure_at_pos_test.go
    structure_test.go
    token_test.go
    variables_test.go
    walk_test.go
)

END()

RECURSE(
    fuzz
    gotest
)
