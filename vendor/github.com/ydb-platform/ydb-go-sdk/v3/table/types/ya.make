GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    cast.go
    errors.go
    types.go
    value.go
)

GO_TEST_SRCS(cast_test.go)

END()

RECURSE(
    gotest
)
