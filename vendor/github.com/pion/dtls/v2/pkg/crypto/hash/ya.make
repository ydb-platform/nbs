GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    hash.go
)

GO_TEST_SRCS(hash_test.go)

END()

RECURSE(
    gotest
)
