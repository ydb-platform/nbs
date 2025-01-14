GO_LIBRARY()

SRCS(
    monitoring.go
)

GO_TEST_SRCS(
    monitoring_test.go
)
END()

RECURSE_FOR_TESTS(
    tests
)
