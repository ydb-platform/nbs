GO_LIBRARY()

SRCS(
    bandwidth_limiter.go
    encrypt.go
    load.go
    s3.go
    source.go
)

GO_TEST_SRCS(
    bandwidth_limiter_test.go
    load_test.go
    s3_test.go
    source_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
