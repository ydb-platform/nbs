GO_LIBRARY()

GO_TEST_SRCS(
    traversal_test.go
)

SRCS(
    traversal.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    tests
)
