GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    x.go
)

GO_TEST_SRCS(x_test.go)

END()

RECURSE(
    gotest
)
