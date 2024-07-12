GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    null.go
)

GO_TEST_SRCS(null_test.go)

END()

RECURSE(
    gotest
)
