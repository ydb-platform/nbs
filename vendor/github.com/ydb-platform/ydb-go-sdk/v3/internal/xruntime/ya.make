GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    cleanup.go
)

GO_TEST_SRCS(cleanup_test.go)

END()

RECURSE(
    gotest
)
