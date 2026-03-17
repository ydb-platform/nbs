GO_LIBRARY()

SRCS(
    metrics.go
    metrics_impl.go
)

END()

RECURSE_FOR_TESTS(
    mocks
)
