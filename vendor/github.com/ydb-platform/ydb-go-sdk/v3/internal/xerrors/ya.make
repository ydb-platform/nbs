GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    check.go
    issues.go
    join.go
    operation.go
    retryable.go
    session.go
    stacktrace.go
    transport.go
    tx.go
    type.go
    xerrors.go
    ydb.go
)

GO_TEST_SRCS(
    issues_test.go
    join_test.go
    operation_test.go
    retryable_test.go
    session_test.go
    stacktrace_test.go
    transport_test.go
    ydb_test.go
)

END()

RECURSE(
    gotest
)
