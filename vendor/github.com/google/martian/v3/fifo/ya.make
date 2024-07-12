GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    fifo_group.go
)

GO_TEST_SRCS(fifo_group_test.go)

END()

RECURSE(
    gotest
)
