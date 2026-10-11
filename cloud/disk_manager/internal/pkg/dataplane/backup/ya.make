GO_LIBRARY()

SRCS(
    bandwidth_limiter.go
    encrypt.go
    keys.go
    meta.go
    s3.go
)

GO_TEST_SRCS(
    bandwidth_limiter_test.go
    keys_test.go
    meta_test.go
    s3_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
