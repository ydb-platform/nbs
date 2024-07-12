GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config_validator.go
    configure.go
    data_source.go
    doc.go
    metadata.go
    read.go
    schema.go
    validate_config.go
)

END()

RECURSE(
    schema
)
