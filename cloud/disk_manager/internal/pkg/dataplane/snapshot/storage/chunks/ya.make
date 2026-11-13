GO_LIBRARY()

SRCS(
    common.go
    storage.go
    storage_s3.go
    storage_ydb.go
)

GO_TEST_SRCS(
    backup_blob_test.go
    storage_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
