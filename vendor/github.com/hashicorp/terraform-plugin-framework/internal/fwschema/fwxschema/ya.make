GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attribute_plan_modification.go
    attribute_validation.go
    block_plan_modification.go
    block_validation.go
    doc.go
    nested_attribute_object_plan_modification.go
    nested_attribute_object_validation.go
    nested_block_object_plan_modification.go
    nested_block_object_validators.go
)

END()
