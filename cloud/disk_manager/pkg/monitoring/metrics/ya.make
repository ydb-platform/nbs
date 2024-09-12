GO_LIBRARY()

SRCS(
    prometheus.go
    compound_registry.go
    registry_wrapper.go
)

GO_TEST_SRCS(
    prometheus_test.go
)
END()

RECURSE_FOR_TESTS(
    tests
)
