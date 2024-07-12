GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
    serve.go
    server_applyresourcechange.go
    server_configureprovider.go
    server_getproviderschema.go
    server_importresourcestate.go
    server_planresourcechange.go
    server_readdatasource.go
    server_readresource.go
    server_upgraderesourcestate.go
    server_validatedataresourceconfig.go
    server_validateproviderconfig.go
    server_validateresourceconfig.go
)

GO_TEST_SRCS(
    serve_test.go
    server_applyresourcechange_test.go
    server_configureprovider_test.go
    server_getproviderschema_test.go
    server_importresourcestate_test.go
    server_planresourcechange_test.go
    server_readdatasource_test.go
    server_readresource_test.go
    server_upgraderesourcestate_test.go
    server_validatedataresourceconfig_test.go
    server_validateproviderconfig_test.go
    server_validateresourceconfig_test.go
)

END()

RECURSE(
    gotest
)
