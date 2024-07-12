GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    data.go
    data_default.go
    data_description.go
    data_get.go
    data_get_at_path.go
    data_nullify_collection_blocks.go
    data_path_exists.go
    data_path_matches.go
    data_reify_null_collection_blocks.go
    data_set.go
    data_set_at_path.go
    data_terraform_value.go
    data_valid_path_expression.go
    data_value.go
    doc.go
    tftypes_value.go
    value_semantic_equality.go
    value_semantic_equality_bool.go
    value_semantic_equality_float64.go
    value_semantic_equality_int64.go
    value_semantic_equality_list.go
    value_semantic_equality_map.go
    value_semantic_equality_number.go
    value_semantic_equality_object.go
    value_semantic_equality_set.go
    value_semantic_equality_string.go
)

GO_XTEST_SRCS(
    data_default_test.go
    data_get_at_path_test.go
    data_get_test.go
    data_nullify_collection_blocks_test.go
    data_path_exists_test.go
    data_path_matches_test.go
    data_reify_null_collection_blocks_test.go
    data_set_at_path_test.go
    data_set_test.go
    data_valid_path_expression_test.go
    data_value_test.go
    pointer_test.go
    tftypes_value_test.go
    value_semantic_equality_bool_test.go
    value_semantic_equality_float64_test.go
    value_semantic_equality_int64_test.go
    value_semantic_equality_list_test.go
    value_semantic_equality_map_test.go
    value_semantic_equality_number_test.go
    value_semantic_equality_object_test.go
    value_semantic_equality_set_test.go
    value_semantic_equality_string_test.go
    value_semantic_equality_test.go
)

END()

RECURSE(
    gotest
)
