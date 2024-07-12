GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    applyresourcechange.go
    config.go
    configureprovider.go
    doc.go
    dynamic_value.go
    getproviderschema.go
    importresourcestate.go
    plan.go
    planresourcechange.go
    prepareproviderconfig.go
    providermeta.go
    readdatasource.go
    readresource.go
    state.go
    upgraderesourcestate.go
    validatedatasourceconfig.go
    validateresourcetypeconfig.go
)

GO_XTEST_SRCS(
    applyresourcechange_test.go
    config_test.go
    configureprovider_test.go
    dynamic_value_test.go
    getproviderschema_test.go
    importresourcestate_test.go
    plan_test.go
    planresourcechange_test.go
    prepareproviderconfig_test.go
    providermeta_test.go
    rawstate_test.go
    readdatasource_test.go
    readresource_test.go
    state_test.go
    upgraderesourcestate_test.go
    validatedatasourceconfig_test.go
    validateresourcetypeconfig_test.go
)

END()

RECURSE(
    gotest
)
