GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    decimal.go
    errors.go
    type.go
)

GO_TEST_SRCS(decimal_test.go)

END()

RECURSE(
    gotest
)
