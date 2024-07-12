GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    attr_value.go
    attribute_plan_modification.go
    attribute_validation.go
    block_plan_modification.go
    block_validation.go
    diagnostics.go
    doc.go
    schema_plan_modification.go
    schema_semantic_equality.go
    schema_validation.go
    server.go
    server_applyresourcechange.go
    server_capabilities.go
    server_configureprovider.go
    server_createresource.go
    server_deleteresource.go
    server_getproviderschema.go
    server_importresourcestate.go
    server_planresourcechange.go
    server_readdatasource.go
    server_readresource.go
    server_updateresource.go
    server_upgraderesourcestate.go
    server_validatedatasourceconfig.go
    server_validateproviderconfig.go
    server_validateresourceconfig.go
)

GO_TEST_SRCS(
    attribute_plan_modification_test.go
    attribute_validation_test.go
    block_plan_modification_test.go
    block_validation_test.go
    schema_plan_modification_test.go
    schema_validation_test.go
)

GO_XTEST_SRCS(
    proto6_test.go
    schema_semantic_equality_test.go
    server_applyresourcechange_test.go
    server_configureprovider_test.go
    server_createresource_test.go
    server_deleteresource_test.go
    server_getproviderschema_test.go
    server_importresourcestate_test.go
    server_planresourcechange_test.go
    server_readdatasource_test.go
    server_readresource_test.go
    server_updateresource_test.go
    server_upgraderesourcestate_test.go
    server_validatedatasourceconfig_test.go
    server_validateproviderconfig_test.go
    server_validateresourceconfig_test.go
)

END()

RECURSE(
    gotest
)
