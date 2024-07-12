GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_GetRoleCredentials.go
    api_op_ListAccountRoles.go
    api_op_ListAccounts.go
    api_op_Logout.go
    auth.go
    deserializers.go
    doc.go
    endpoints.go
    go_module_metadata.go
    options.go
    serializers.go
    validators.go
)

GO_TEST_SRCS(
    api_client_test.go
    endpoints_config_test.go
    endpoints_test.go
    protocol_test.go
)

END()

RECURSE(
    gotest
    internal
    types
)
