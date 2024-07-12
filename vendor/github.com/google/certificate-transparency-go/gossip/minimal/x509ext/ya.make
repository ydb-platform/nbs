GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    x509ext.go
)

GO_XTEST_SRCS(x509ext_test.go)

END()

RECURSE(
    gotest
)
