GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    messageview.go
)

GO_TEST_SRCS(messageview_test.go)

END()

RECURSE(
    gotest
)
