GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    test.go
)

GO_TEST_SRCS(test_test.go)

END()

RECURSE(
    gotest
)
