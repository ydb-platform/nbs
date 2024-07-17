GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    stdr.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
