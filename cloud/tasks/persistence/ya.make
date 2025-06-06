GO_LIBRARY()

SRCS(
    s3.go
    s3_metrics.go
    ydb.go
    ydb_logger.go
    ydb_metrics.go

    availability_monitoring.go
    availability_monitoring_storage.go
)

GO_TEST_SRCS(
    s3_test.go
    ydb_test.go

    availability_monitoring_test.go
    availability_monitoring_storage_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
