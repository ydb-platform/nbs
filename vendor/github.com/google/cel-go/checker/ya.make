GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    checker.go
    cost.go
    env.go
    errors.go
    format.go
    mapping.go
    options.go
    printer.go
    scopes.go
    standard.go
    types.go
)

GO_TEST_SRCS(
    checker_test.go
    cost_test.go
    env_test.go
    format_test.go
)

END()

RECURSE(
    decls
    gotest
)
