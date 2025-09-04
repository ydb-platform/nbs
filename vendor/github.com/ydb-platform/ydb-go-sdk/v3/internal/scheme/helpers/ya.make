GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    check_exists.go
)

GO_TEST_SRCS(check_exists_test.go)

END()

RECURSE(
    gotest
)
