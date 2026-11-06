GO_LIBRARY()

SRCS(
    schema.go
)

GO_TEST_SRCS(
    backup_schema_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
