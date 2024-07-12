GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    behaviors.go
    render.go
    write_attribute_description.go
    write_block_type_description.go
    write_nested_attribute_type_description.go
    write_type.go
)

GO_TEST_SRCS(behaviors_test.go)

GO_XTEST_SRCS(
    render_test.go
    write_attribute_description_test.go
    write_block_type_description_test.go
    write_nested_attribute_type_description_test.go
    write_type_test.go
)

END()

RECURSE(
    gotest
)
