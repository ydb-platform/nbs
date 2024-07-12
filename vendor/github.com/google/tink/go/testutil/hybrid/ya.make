GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    private_key.go
)

GO_XTEST_SRCS(private_key_test.go)

END()

RECURSE(
    gotest
)
