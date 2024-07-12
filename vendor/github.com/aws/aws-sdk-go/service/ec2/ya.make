GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api.go
    customizations.go
    doc.go
    errors.go
    service.go
    waiters.go
)

GO_TEST_SRCS(retryer_test.go)

GO_XTEST_SRCS(
    customizations_test.go
    examples_test.go
)

END()

RECURSE(
    ec2iface
    gotest
)
