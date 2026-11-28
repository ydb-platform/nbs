GO_LIBRARY()

SRCS(
    target.go
)

GO_TEST_SRCS(
    target_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
