GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    decimal.go
    errors.go
)

GO_TEST_SRCS(decimal_test.go)

END()

RECURSE(
    gotest
)
