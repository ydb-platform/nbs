GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    test.go
)

GO_TEST_SRCS(test_test.go)

END()

RECURSE(
    gotest
)
