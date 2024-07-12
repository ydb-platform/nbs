GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    mock_clientauth.go
)

GO_TEST_SRCS(mock_clientauth_test.go)

END()

RECURSE(
    gotest
)
