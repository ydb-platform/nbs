GO_LIBRARY()

SRCS(
    common.go
    storage.go
    storage_ydb.go
)

GO_TEST_SRCS(
    storage_ydb_test.go
)

END()

RECURSE(
    mocks
    protos
    schema
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
