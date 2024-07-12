GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    cost.go
    doc.go
    error.go
    errors.go
    location.go
    source.go
)

GO_TEST_SRCS(
    errors_test.go
    source_test.go
)

END()

RECURSE(
    ast
    containers
    debug
    decls
    functions
    gotest
    operators
    overloads
    runes
    stdlib
    types
)
