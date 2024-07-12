GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    coerce_value.go
    doc.go
    empty_value.go
    implied_type.go
    nestingmode_string.go
    schema.go
)

GO_TEST_SRCS(
    coerce_value_test.go
    empty_value_test.go
    implied_type_test.go
)

END()

RECURSE(
    gotest
)
