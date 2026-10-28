GO_LIBRARY()

SRCS(
    encrypt.go
    keys.go
    meta.go
    s3.go
)

GO_TEST_SRCS(
    meta_failure_test.go
    keys_test.go
    meta_test.go
    s3_test.go
    s3_http_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
