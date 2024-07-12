GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    buffer.go
)

GO_TEST_SRCS(buffer_test.go)

END()

RECURSE(
    gotest
)
