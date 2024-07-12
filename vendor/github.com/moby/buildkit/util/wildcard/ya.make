GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    wildcard.go
)

GO_TEST_SRCS(wildcard_test.go)

END()

RECURSE(
    gotest
)
