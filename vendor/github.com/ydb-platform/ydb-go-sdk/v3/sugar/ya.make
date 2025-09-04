GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    certificates.go
    check_exists.go
    dsn.go
    errors.go
    params.go
    path.go
    query.go
    result.go
    stack.go
)

GO_TEST_SRCS(
    dsn_test.go
    errors_test.go
    params_test.go
    stack_test.go
)

GO_XTEST_SRCS(
    example_test.go
    query_iterators_test.go
    query_test.go
)

END()

RECURSE(
    gotest
)
