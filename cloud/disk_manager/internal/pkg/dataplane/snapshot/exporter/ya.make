GO_LIBRARY()

SRCS(
    export.go
    s3.go
    s3_checkpoint.go
    s3_compression.go
    s3_parts.go
    s3_progress.go
    s3_retry.go
    source.go
)

GO_TEST_SRCS(
    export_test.go
    partition_helpers_test.go
    partition_integrity_test.go
    partition_map_test.go
    partition_range_test.go
    s3_test.go
    s3_checkpoint_test.go
    s3_compression_test.go
    s3_progress_test.go
    s3_retry_test.go
    source_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
