GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    any_value.go
    bool.go
    bytes.go
    compare.go
    doc.go
    double.go
    duration.go
    err.go
    int.go
    iterator.go
    json_value.go
    list.go
    map.go
    null.go
    object.go
    optional.go
    overflow.go
    provider.go
    string.go
    timestamp.go
    types.go
    uint.go
    unknown.go
    util.go
)

GO_TEST_SRCS(
    bool_test.go
    bytes_test.go
    double_test.go
    duration_test.go
    int_test.go
    json_list_test.go
    json_struct_test.go
    list_test.go
    map_test.go
    null_test.go
    object_test.go
    optional_test.go
    provider_test.go
    string_test.go
    timestamp_test.go
    type_test.go
    types_test.go
    uint_test.go
    unknown_test.go
    util_test.go
)

END()

RECURSE(
    gotest
    pb
    ref
    traits
)
