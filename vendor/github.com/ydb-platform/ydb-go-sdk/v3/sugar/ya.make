GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    certificates.go
    check_exists.go
    dsn.go
    params.go
    path.go
    query.go
    result.go
    stack.go
)

GO_TEST_SRCS(
    dsn_test.go
    params_test.go
    stack_test.go
)

GO_XTEST_SRCS(
    example_test.go
    query_test.go
)

END()

RECURSE(
    gotest
)
