GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cast.go
    time.go
    type.go
    value.go
)

GO_TEST_SRCS(
    time_test.go
    type_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
