GO_LIBRARY()

SRCS(
    export.go
)

GO_TEST_SRCS(
    export_test.go
    partition_helpers_test.go
    partition_integrity_test.go
    partition_map_test.go
    partition_range_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
