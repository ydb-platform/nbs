GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    user.go
)

GO_XTEST_SRCS(
    mock_user_test.go
    user_test.go
)

END()

RECURSE(
    concurrent
    gotest
    imp1
    imp2
    imp3
    imp4
)
