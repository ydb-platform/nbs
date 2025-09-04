GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    backoff.go
    context.go
    mode.go
    retry.go
    retryable_error.go
    sql.go
)

GO_TEST_SRCS(
    errors_data_test.go
    retry_test.go
    sql_test.go
)

END()

RECURSE(
    budget
    gotest
)
