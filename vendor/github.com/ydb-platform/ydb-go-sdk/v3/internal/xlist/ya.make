GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    list.go
)

GO_TEST_SRCS(list_test.go)

END()

RECURSE(
    gotest
)
