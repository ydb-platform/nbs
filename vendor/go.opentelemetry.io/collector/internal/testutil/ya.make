GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    testutil.go
)

GO_TEST_SRCS(testutil_test.go)

END()

RECURSE(
    gotest
)
