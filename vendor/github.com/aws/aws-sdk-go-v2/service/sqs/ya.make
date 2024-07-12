GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_AddPermission.go
    api_op_ChangeMessageVisibility.go
    api_op_ChangeMessageVisibilityBatch.go
    api_op_CreateQueue.go
    api_op_DeleteMessage.go
    api_op_DeleteMessageBatch.go
    api_op_DeleteQueue.go
    api_op_GetQueueAttributes.go
    api_op_GetQueueUrl.go
    api_op_ListDeadLetterSourceQueues.go
    api_op_ListQueueTags.go
    api_op_ListQueues.go
    api_op_PurgeQueue.go
    api_op_ReceiveMessage.go
    api_op_RemovePermission.go
    api_op_SendMessage.go
    api_op_SendMessageBatch.go
    api_op_SetQueueAttributes.go
    api_op_TagQueue.go
    api_op_UntagQueue.go
    deserializers.go
    doc.go
    endpoints.go
    go_module_metadata.go
    serializers.go
    validators.go
)

GO_TEST_SRCS(protocol_test.go)

END()

RECURSE(
    gotest
    internal
    types
)
