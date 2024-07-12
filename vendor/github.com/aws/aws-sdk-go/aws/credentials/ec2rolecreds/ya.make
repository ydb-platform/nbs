GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ec2_role_provider.go
)

GO_XTEST_SRCS(ec2_role_provider_test.go)

END()

RECURSE(
    gotest
)
