GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    sts.go
)

GO_TEST_SRCS(sts_test.go)

END()

RECURSE(
    gotest
)
