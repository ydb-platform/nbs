GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config_validator.go
    configure.go
    doc.go
    metadata.go
    metaschema.go
    provider.go
    schema.go
    validate_config.go
)

END()

RECURSE(
    metaschema
    schema
)
