GO_LIBRARY()

SRCS(
    keys.go
    meta.go
)

GO_TEST_SRCS(
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
