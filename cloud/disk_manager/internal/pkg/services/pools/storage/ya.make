GO_LIBRARY()

SRCS(
    common.go
    consistency_check.go
    storage.go
    storage_ydb.go
    storage_ydb_impl.go
)

GO_TEST_SRCS(
    common_test.go
    storage_ydb_test.go
)

END()

RECURSE(
    metrics
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
