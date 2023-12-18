GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    isolation.go
)

GO_TEST_SRCS(isolation_test.go)

END()

RECURSE(
    gotest
)
