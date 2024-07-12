GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    lru.go
    lru_interface.go
)

GO_TEST_SRCS(lru_test.go)

END()

RECURSE(
    gotest
)
