GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    integer.go
)

GO_TEST_SRCS(integer_test.go)

END()

RECURSE(
    gotest
)
