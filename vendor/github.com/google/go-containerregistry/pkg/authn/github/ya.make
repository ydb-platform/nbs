GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    keychain.go
)

GO_TEST_SRCS(keychain_test.go)

END()

RECURSE(
    gotest
)
