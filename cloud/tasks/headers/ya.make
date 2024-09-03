GO_LIBRARY()

SRCS(
    headers.go
)

GO_TEST_SRCS(
    headers_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
