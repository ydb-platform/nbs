GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(backoff.go)

GO_XTEST_SRCS(backoff_test.go)

END()

RECURSE(
    # gotest
)