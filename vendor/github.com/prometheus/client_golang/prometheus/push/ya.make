GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    push.go
)

GO_TEST_SRCS(push_test.go)

GO_XTEST_SRCS(
    example_add_from_gatherer_test.go
    examples_test.go
)

END()

RECURSE(
    gotest
)
