GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    field.go
)

GO_TEST_SRCS(field_test.go)

END()

RECURSE(
    gotest
)
