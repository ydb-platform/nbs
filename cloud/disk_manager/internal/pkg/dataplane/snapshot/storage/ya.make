GO_LIBRARY()

SRCS(
    common.go
    factory.go
    storage.go
    storage_legacy.go
    storage_legacy_impl.go
    storage_ydb.go
    storage_ydb_impl.go
    storage_ydb_metrics.go
)

GO_TEST_SRCS(
    hanging_ydb_test.go
)

END()

RECURSE(
    chunks
    compressor
    metrics
    protos
    schema
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
