GO_LIBRARY()

SRCS(
    storage.go
    storage_ydb.go
)

GO_TEST_SRCS(
    storage_ydb_test.go
)

END()

RECURSE(
)

RECURSE_FOR_TESTS(
    tests
)
