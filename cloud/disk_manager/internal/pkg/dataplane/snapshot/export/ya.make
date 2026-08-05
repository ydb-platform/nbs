GO_LIBRARY()

SRCS(
    export.go
)

GO_TEST_SRCS(
    export_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
