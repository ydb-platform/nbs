GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    expirable_lru.go
)

GO_TEST_SRCS(expirable_lru_test.go)

END()

RECURSE(
    gotest
)
