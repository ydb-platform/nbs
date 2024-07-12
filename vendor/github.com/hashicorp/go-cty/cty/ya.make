GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    capsule.go
    capsule_ops.go
    collection.go
    doc.go
    element_iterator.go
    error.go
    gob.go
    helper.go
    json.go
    list_type.go
    map_type.go
    marks.go
    null.go
    object_type.go
    path.go
    path_set.go
    primitive_type.go
    set_helper.go
    set_internals.go
    set_type.go
    tuple_type.go
    type.go
    type_conform.go
    types_to_register.go
    unknown.go
    unknown_as_null.go
    value.go
    value_init.go
    value_ops.go
    walk.go
)

GO_TEST_SRCS(
    capsule_test.go
    gob_test.go
    json_test.go
    marks_test.go
    object_type_test.go
    path_set_test.go
    primitive_type_test.go
    set_internals_test.go
    set_type_test.go
    tuple_type_test.go
    type_conform_test.go
    unknown_as_null_test.go
    value_init_test.go
    value_ops_test.go
    walk_test.go
)

GO_XTEST_SRCS(path_test.go)

END()

RECURSE(
    convert
    gocty
    gotest
    json
    msgpack
    set
)
