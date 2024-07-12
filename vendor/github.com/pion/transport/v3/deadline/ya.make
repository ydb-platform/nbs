GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    deadline.go
)

GO_TEST_SRCS(deadline_test.go)

END()

RECURSE(
    gotest
)
