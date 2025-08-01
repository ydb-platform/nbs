GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    errors.go
    execute_query.go
    range_experiment.go
    result.go
    result_set.go
    row.go
    session.go
    session_status.go
    transaction.go
)

GO_TEST_SRCS(
    client_test.go
    execute_query_test.go
    grpc_client_mock_test.go
    result_set_test.go
    result_test.go
    session_test.go
    transaction_test.go
)

END()

RECURSE(
    config
    gotest
    options
    scanner
    tx
)
