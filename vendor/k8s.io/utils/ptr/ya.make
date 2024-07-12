GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    ptr.go
)

GO_XTEST_SRCS(ptr_test.go)

END()

RECURSE(
    gotest
)
