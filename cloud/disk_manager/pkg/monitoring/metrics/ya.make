GO_LIBRARY()

SRCS(
    prometheus.go
)

GO_TEST_SRCS(
    prometheus_test.go
)
END()

RECURSE_FOR_TESTS(
    tests
)
