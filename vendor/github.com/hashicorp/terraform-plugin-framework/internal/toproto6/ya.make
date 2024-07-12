GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    applyresourcechange.go
    block.go
    config.go
    configureprovider.go
    diagnostics.go
    doc.go
    dynamic_value.go
    getproviderschema.go
    importedresource.go
    importresourcestate.go
    planresourcechange.go
    readdatasource.go
    readresource.go
    schema.go
    schema_attribute.go
    server_capabilities.go
    state.go
    upgraderesourcestate.go
    validatedatasourceconfig.go
    validateproviderconfig.go
    validateresourceconfig.go
)

GO_XTEST_SRCS(
    applyresourcechange_test.go
    block_test.go
    config_test.go
    configureprovider_test.go
    diagnostics_test.go
    dynamic_value_test.go
    getproviderschema_test.go
    importedresource_test.go
    planresourcechange_test.go
    readdatasource_test.go
    readresource_test.go
    schema_attribute_test.go
    schema_test.go
    server_capabilities_test.go
    state_test.go
    upgraderesourcestate_test.go
    validatedatasourceconfig_test.go
    validateproviderconfig_test.go
    validateresourceconfig_test.go
)

END()

RECURSE(
    gotest
)
