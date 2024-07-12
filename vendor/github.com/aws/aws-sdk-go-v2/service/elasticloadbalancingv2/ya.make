GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_AddListenerCertificates.go
    api_op_AddTags.go
    api_op_CreateListener.go
    api_op_CreateLoadBalancer.go
    api_op_CreateRule.go
    api_op_CreateTargetGroup.go
    api_op_DeleteListener.go
    api_op_DeleteLoadBalancer.go
    api_op_DeleteRule.go
    api_op_DeleteTargetGroup.go
    api_op_DeregisterTargets.go
    api_op_DescribeAccountLimits.go
    api_op_DescribeListenerCertificates.go
    api_op_DescribeListeners.go
    api_op_DescribeLoadBalancerAttributes.go
    api_op_DescribeLoadBalancers.go
    api_op_DescribeRules.go
    api_op_DescribeSSLPolicies.go
    api_op_DescribeTags.go
    api_op_DescribeTargetGroupAttributes.go
    api_op_DescribeTargetGroups.go
    api_op_DescribeTargetHealth.go
    api_op_ModifyListener.go
    api_op_ModifyLoadBalancerAttributes.go
    api_op_ModifyRule.go
    api_op_ModifyTargetGroup.go
    api_op_ModifyTargetGroupAttributes.go
    api_op_RegisterTargets.go
    api_op_RemoveListenerCertificates.go
    api_op_RemoveTags.go
    api_op_SetIpAddressType.go
    api_op_SetRulePriorities.go
    api_op_SetSecurityGroups.go
    api_op_SetSubnets.go
    deserializers.go
    doc.go
    endpoints.go
    go_module_metadata.go
    serializers.go
    validators.go
)

GO_TEST_SRCS(
    api_client_test.go
    protocol_test.go
)

END()

RECURSE(
    gotest
    internal
    types
)
