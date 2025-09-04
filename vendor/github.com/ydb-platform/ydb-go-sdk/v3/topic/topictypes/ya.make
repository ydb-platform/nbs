GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    topictypes.go
)

GO_TEST_SRCS(topictypes_test.go)

END()

RECURSE(
    gotest
)
