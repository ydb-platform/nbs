GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    zstd.go
)

GO_TEST_SRCS(zstd_test.go)

END()

RECURSE(
    gotest
)
