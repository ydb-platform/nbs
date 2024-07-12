GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    auxiliary.go
    doc.go
    policies_client.go
    version.go
)

GO_XTEST_SRCS(policies_client_example_test.go)

END()

RECURSE(
    gotest
    iampb
)
