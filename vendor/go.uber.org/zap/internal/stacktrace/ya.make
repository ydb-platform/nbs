GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    stack.go
)

GO_TEST_SRCS(stack_test.go)

END()

RECURSE(
    gotest
)
