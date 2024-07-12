GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_BatchGetSecretValue.go
    api_op_CancelRotateSecret.go
    api_op_CreateSecret.go
    api_op_DeleteResourcePolicy.go
    api_op_DeleteSecret.go
    api_op_DescribeSecret.go
    api_op_GetRandomPassword.go
    api_op_GetResourcePolicy.go
    api_op_GetSecretValue.go
    api_op_ListSecretVersionIds.go
    api_op_ListSecrets.go
    api_op_PutResourcePolicy.go
    api_op_PutSecretValue.go
    api_op_RemoveRegionsFromReplication.go
    api_op_ReplicateSecretToRegions.go
    api_op_RestoreSecret.go
    api_op_RotateSecret.go
    api_op_StopReplicationToReplica.go
    api_op_TagResource.go
    api_op_UntagResource.go
    api_op_UpdateSecret.go
    api_op_UpdateSecretVersionStage.go
    api_op_ValidateResourcePolicy.go
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
