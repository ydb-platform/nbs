GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    query.go
)

GO_TEST_SRCS(
    query_go1.23_test.go
    query_test.go
)

END()

RECURSE(
    gotest
)
