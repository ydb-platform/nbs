GO_LIBRARY()

SRCS(
    storage.go
    storage_ydb.go
)

GO_TEST_SRCS(
    storage_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
