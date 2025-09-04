GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    dsn.go
)

GO_TEST_SRCS(dsn_test.go)

END()

RECURSE(
    gotest
)
