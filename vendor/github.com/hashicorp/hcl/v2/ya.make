GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diagnostic.go
    diagnostic_text.go
    diagnostic_typeparams.go
    didyoumean.go
    doc.go
    eval_context.go
    expr_call.go
    expr_list.go
    expr_map.go
    expr_unwrap.go
    merged.go
    ops.go
    pos.go
    pos_scanner.go
    schema.go
    static_expr.go
    structure.go
    structure_at_pos.go
    traversal.go
    traversal_for_expr.go
)

GO_TEST_SRCS(
    diagnostic_text_test.go
    merged_test.go
    ops_test.go
    pos_scanner_test.go
    pos_test.go
    traversal_for_expr_test.go
)

END()

RECURSE(
    cmd
    ext
    gohcl
    gotest
    hcldec
    hcled
    hclparse
    hclsimple
    hclsyntax
    hcltest
    hclwrite
    integrationtest
    json
    # specsuite
)
