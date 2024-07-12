GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute.go
    attribute_default.go
    attribute_name_validation.go
    attribute_nesting_mode.go
    attribute_validate_implementation.go
    block.go
    block_nested_mode.go
    block_validate_implementation.go
    diagnostics.go
    doc.go
    errors.go
    nested_attribute.go
    nested_attribute_object.go
    nested_block_object.go
    schema.go
    underlying_attributes.go
    validate_implementation.go
)

GO_XTEST_SRCS(
    attribute_name_validation_test.go
    schema_test.go
)

END()

RECURSE(
    fwxschema
    gotest
)
