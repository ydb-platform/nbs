GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    iam.go
)

GO_TEST_SRCS(iam_test.go)

END()

RECURSE(
    admin
    apiv1
    apiv2
    credentials
    gotest
    internal
)
