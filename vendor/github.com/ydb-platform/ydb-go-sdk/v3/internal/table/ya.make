GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    data_query.go
    errors.go
    params.go
    retry.go
    session.go
    statement.go
    transaction.go
    ttl.go
)

GO_TEST_SRCS(
    client_test.go
    retry_test.go
    session_test.go
    transaction_test.go
)

END()

RECURSE(
    config
    gotest
    scanner
)
