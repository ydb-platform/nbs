GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cast.go
    types.go
    value.go
)

GO_TEST_SRCS(cast_test.go)

END()

RECURSE(
    gotest
)
