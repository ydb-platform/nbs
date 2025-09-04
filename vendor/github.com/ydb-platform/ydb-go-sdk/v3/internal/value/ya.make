GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    any.go
    cast.go
    errors.go
    nullable.go
    time.go
    value.go
)

GO_TEST_SRCS(
    any_test.go
    cast_test.go
    time_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
