GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    expand.go
)

GO_TEST_SRCS(expand_test.go)

END()

RECURSE(
    gotest
)
