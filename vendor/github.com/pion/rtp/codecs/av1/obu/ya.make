GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    leb128.go
)

GO_TEST_SRCS(leb128_test.go)

END()

RECURSE(
    gotest
)
