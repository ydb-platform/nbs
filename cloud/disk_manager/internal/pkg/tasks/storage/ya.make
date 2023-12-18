GO_LIBRARY()

SRCS(
    common.go
    metrics.go
    node.go
    compound_storage.go
    storage.go
    storage_ydb.go
    storage_ydb_impl.go
)

GO_TEST_SRCS(
    storage_ydb_test.go
)

END()

RECURSE_FOR_TESTS(
    mocks
    tests
)
