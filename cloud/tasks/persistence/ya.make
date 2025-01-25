GO_LIBRARY()

SRCS(
    health.go
    s3.go
    s3_metrics.go
    ydb.go
    ydb_logger.go
    ydb_metrics.go
)

GO_TEST_SRCS(
    health_test.go
    s3_test.go
    ydb_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
