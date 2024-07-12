GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    diags.go
    doc.go
    generic_attr_value.go
    helpers.go
    interfaces.go
    into.go
    map.go
    number.go
    options.go
    outof.go
    pointer.go
    primitive.go
    slice.go
    struct.go
)

GO_TEST_SRCS(
    # helpers_test.go
    # pointer_zero_test.go
)

GO_XTEST_SRCS(
    # build_value_test.go
    # interfaces_test.go
    # map_test.go
    # number_test.go
    # pointer_test.go
    # primitive_test.go
    # struct_test.go
)

END()

RECURSE(
    gotest
)
