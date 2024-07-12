GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    bool_type.go
    bool_value.go
    doc.go
    float64_type.go
    float64_value.go
    int64_type.go
    int64_value.go
    list_type.go
    list_value.go
    map_type.go
    map_value.go
    number_type.go
    number_value.go
    object_type.go
    object_value.go
    set_type.go
    set_value.go
    string_type.go
    string_value.go
)

GO_TEST_SRCS(
    bool_type_test.go
    bool_value_test.go
    float64_type_test.go
    float64_value_test.go
    int64_type_test.go
    int64_value_test.go
    list_type_test.go
    list_value_test.go
    map_type_test.go
    map_value_test.go
    number_type_test.go
    number_value_test.go
    object_type_test.go
    object_value_test.go
    pointer_test.go
    set_type_test.go
    set_value_test.go
    string_type_test.go
    string_value_test.go
)

END()

RECURSE(
    gotest
)
