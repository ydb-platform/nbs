GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auxiliary.go
    doc.go
    iam_policy_client.go
    version.go
)

GO_XTEST_SRCS(iam_policy_client_example_test.go)

END()

RECURSE(
    gotest
    iampb
)
