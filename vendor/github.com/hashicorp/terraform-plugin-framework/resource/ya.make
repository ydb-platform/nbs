GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    config_validator.go
    configure.go
    create.go
    delete.go
    doc.go
    import_state.go
    metadata.go
    modify_plan.go
    read.go
    resource.go
    schema.go
    state_upgrader.go
    update.go
    upgrade_state.go
    validate_config.go
)

END()

RECURSE(
    schema
)
