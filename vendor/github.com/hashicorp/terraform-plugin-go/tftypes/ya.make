GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute_path.go
    attribute_path_error.go
    diff.go
    doc.go
    list.go
    map.go
    object.go
    primitive.go
    set.go
    tuple.go
    type.go
    unknown_value.go
    value.go
    value_json.go
    value_msgpack.go
    walk.go
)

GO_TEST_SRCS(
    attribute_path_error_test.go
    attribute_path_test.go
    diff_test.go
    list_test.go
    map_test.go
    object_test.go
    primitive_test.go
    set_test.go
    tuple_test.go
    type_json_test.go
    value_dpt_test.go
    value_json_test.go
    value_list_test.go
    value_map_test.go
    value_msgpack_test.go
    value_number_test.go
    value_object_test.go
    value_set_test.go
    value_test.go
    value_tuple_test.go
    walk_test.go
)

GO_XTEST_SRCS(value_example_test.go)

END()

RECURSE(
    gotest
)
