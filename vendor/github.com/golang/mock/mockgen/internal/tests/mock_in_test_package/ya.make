GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    user.go
)

GO_XTEST_SRCS(mock_test.go)

END()

RECURSE(
    gotest
)
