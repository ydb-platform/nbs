GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    binaryutil.go
)

GO_TEST_SRCS(binaryutil_test.go)

END()

RECURSE(
    gotest
)
