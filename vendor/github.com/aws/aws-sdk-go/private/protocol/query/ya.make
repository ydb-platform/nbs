GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    build.go
    unmarshal.go
    unmarshal_error.go
)

GO_TEST_SRCS(unmarshal_error_test.go)

GO_XTEST_SRCS(
    build_test.go
    unmarshal_test.go
)

END()

RECURSE(
    # gotest
    queryutil
)
