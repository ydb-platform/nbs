GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    iam_client.go
    path_funcs.go
    policy_methods.go
    version.go
)

GO_XTEST_SRCS(iam_client_example_test.go)

END()

RECURSE(
    adminpb
    gotest
)
