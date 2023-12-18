GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    check.go
    issues.go
    join.go
    operation.go
    retryable.go
    stacktrace.go
    transport.go
    type.go
    xerrors.go
    ydb.go
)

GO_TEST_SRCS(
    join_test.go
    operation_test.go
    pessimized_error_test.go
    retryable_test.go
    stacktrace_test.go
    transport_test.go
    ydb_test.go
)

END()

RECURSE(
    gotest
)
