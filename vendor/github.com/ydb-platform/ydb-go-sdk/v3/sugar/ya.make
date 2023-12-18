GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    certificates.go
    check_exists.go
    dsn.go
    params_go1.18.go
    path.go
    stack.go
)

GO_TEST_SRCS(
    dsn_test.go
    params_go1.18_test.go
    params_test.go
    stack_test.go
)

END()

RECURSE(
    gotest
)
