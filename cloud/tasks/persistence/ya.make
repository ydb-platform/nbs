GO_LIBRARY()

SRCS(
    s3.go
    s3_metrics.go
    ydb.go
    ydb_logger.go
    ydb_metrics.go

    health.go
    health_storage.go
    health_storage_mocks.go
)

GO_TEST_SRCS(
    s3_test.go
    ydb_test.go
    health_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
