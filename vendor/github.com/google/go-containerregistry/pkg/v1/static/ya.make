GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    layer.go
)

GO_TEST_SRCS(static_test.go)

END()

RECURSE(
    gotest
)
