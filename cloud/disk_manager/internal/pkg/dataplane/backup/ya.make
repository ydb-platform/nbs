GO_LIBRARY()

SRCS(
    keys.go
    meta.go
    follower_s3.go
)

GO_TEST_SRCS(
    follower_s3_test.go
    keys_test.go
    meta_test.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
