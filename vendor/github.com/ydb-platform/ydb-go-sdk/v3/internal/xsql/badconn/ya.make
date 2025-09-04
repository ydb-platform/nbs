GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    badconn.go
)

GO_TEST_SRCS(badconn_test.go)

END()

RECURSE(
    gotest
)
