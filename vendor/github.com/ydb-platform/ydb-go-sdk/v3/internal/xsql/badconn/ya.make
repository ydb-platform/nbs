GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    badconn_go1.18.go
)

GO_TEST_SRCS(badconn_go1.18_test.go)

END()

RECURSE(
    gotest
)
