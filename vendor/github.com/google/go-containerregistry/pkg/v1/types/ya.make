GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    types.go
)

GO_TEST_SRCS(types_test.go)

END()

RECURSE(
    gotest
)
