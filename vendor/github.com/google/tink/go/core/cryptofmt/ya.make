GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    cryptofmt.go
)

GO_XTEST_SRCS(cryptofmt_test.go)

END()

RECURSE(
    gotest
)
