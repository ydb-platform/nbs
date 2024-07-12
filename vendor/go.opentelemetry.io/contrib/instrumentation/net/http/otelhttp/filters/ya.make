GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    filters.go
    header.go
)

GO_TEST_SRCS(filters_test.go)

END()

RECURSE(
    gotest
)
