GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cast.go
    errors.go
    nullable.go
    time.go
    value.go
)

GO_TEST_SRCS(
    cast_test.go
    time_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
