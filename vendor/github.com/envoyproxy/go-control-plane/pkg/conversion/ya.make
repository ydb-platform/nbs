GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    struct.go
)

GO_XTEST_SRCS(struct_test.go)

END()

RECURSE(
    gotest
)
