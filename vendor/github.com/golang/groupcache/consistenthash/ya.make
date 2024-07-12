GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    consistenthash.go
)

GO_TEST_SRCS(consistenthash_test.go)

END()

RECURSE(
    gotest
)
