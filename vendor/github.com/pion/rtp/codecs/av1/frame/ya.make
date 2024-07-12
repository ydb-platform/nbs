GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    av1.go
)

GO_TEST_SRCS(av1_test.go)

END()

RECURSE(
    gotest
)
