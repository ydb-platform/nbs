GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    noncebased.go
)

GO_XTEST_SRCS(noncebased_test.go)

END()

RECURSE(
    gotest
)
