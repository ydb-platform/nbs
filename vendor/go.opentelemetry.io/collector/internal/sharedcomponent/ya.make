GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    sharedcomponent.go
)

GO_TEST_SRCS(sharedcomponent_test.go)

END()

RECURSE(
    gotest
)
