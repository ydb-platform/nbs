GO_LIBRARY()

SRCS(
    common.go
    s3_chunk.go
    storage.go
    storage_s3.go
    storage_ydb.go
)

GO_TEST_SRCS(
    storage_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
