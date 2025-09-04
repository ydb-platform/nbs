GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    builder.go
    dict.go
    list.go
    optional.go
    parameters.go
    pg.go
    set.go
    struct.go
    tuple.go
    variant.go
    variant_struct.go
    variant_tuple.go
)

GO_TEST_SRCS(
    builder_test.go
    dict_test.go
    list_test.go
    optional_test.go
    parameters_test.go
    pg_test.go
    set_test.go
    struct_test.go
    tuple_test.go
    variant_struct_test.go
    variant_tuple_test.go
)

END()

RECURSE(
    gotest
)
