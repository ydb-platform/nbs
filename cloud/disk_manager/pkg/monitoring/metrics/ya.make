GO_LIBRARY()

SRCS(
    prometheus.go
    registry_wrapper.go
)

GO_TEST_SRCS(
    prometheus_test.go
)
END()

RECURSE_FOR_TESTS(
    tests
)
