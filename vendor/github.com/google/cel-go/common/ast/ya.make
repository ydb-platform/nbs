GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    ast.go
    conversion.go
    expr.go
    factory.go
    navigable.go
)

GO_XTEST_SRCS(
    ast_test.go
    conversion_test.go
    expr_test.go
    navigable_test.go
)

END()

RECURSE(
    gotest
)
