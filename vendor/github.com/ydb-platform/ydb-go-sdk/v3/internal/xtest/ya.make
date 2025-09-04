GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    call_method.go
    clock.go
    context.go
    current_file_line.go
    grpclogger.go
    logger.go
    manytimes.go
    must.go
    test_condition.go
    to_json.go
    waiters.go
    ydb_grpc_mocks.go
)

GO_TEST_SRCS(
    call_method_test.go
    current_file_line_test.go
    must_test.go
    to_json_test.go
)

END()

RECURSE(
    gotest
)
