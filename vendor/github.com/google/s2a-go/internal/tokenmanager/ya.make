GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    tokenmanager.go
)

GO_TEST_SRCS(tokenmanager_test.go)

END()

RECURSE(
    gotest
)
