GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    hmac.go
    pool.go
)

GO_TEST_SRCS(
    hmac_test.go
    pool_test.go
)

END()

RECURSE(
    gotest
)
