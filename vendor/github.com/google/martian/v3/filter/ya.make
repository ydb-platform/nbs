GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    condition.go
    filter.go
)

GO_TEST_SRCS(filter_test.go)

END()

RECURSE(
    gotest
)
