GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    ast.go
    walk.go
)

GO_TEST_SRCS(ast_test.go)

END()

RECURSE(
    gotest
)
