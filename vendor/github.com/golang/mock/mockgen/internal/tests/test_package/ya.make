GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    foo.go
)

GO_XTEST_SRCS(
    mock_test.go
    user_test.go
)

END()

RECURSE(
    gotest
)
