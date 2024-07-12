GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    util.go
)

GO_TEST_SRCS(util_test.go)

END()

RECURSE(
    gotest
)
