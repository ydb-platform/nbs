GO_LIBRARY()

SRCS(
    cache.go
)

GO_TEST_SRCS(
    cache_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
