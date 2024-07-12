GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    lru.go
)

GO_TEST_SRCS(lru_test.go)

END()

RECURSE(
    gotest
)
