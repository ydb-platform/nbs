GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    client.go
    errors.go
    execute_query.go
    result.go
    result_set.go
    row.go
    session.go
    session_core.go
    session_status.go
    transaction.go
)

GO_TEST_SRCS(
    client_test.go
    execute_query_test.go
    grpc_client_mock_test.go
    result_go1.23_test.go
    result_set_go1.23_test.go
    result_set_test.go
    result_test.go
    row_test.go
    session_core_test.go
    session_fixtures_test.go
    session_test.go
    transaction_fixtures_test.go
    transaction_test.go
)

END()

RECURSE(
    config
    gotest
    options
    result
    scanner
)
