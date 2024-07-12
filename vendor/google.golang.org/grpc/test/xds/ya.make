GO_TEST()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

GO_XTEST_SRCS(
    xds_client_ack_nack_test.go
    xds_client_affinity_test.go
    xds_client_custom_lb_test.go
    xds_client_federation_test.go
    xds_client_ignore_resource_deletion_test.go
    xds_client_integration_test.go
    xds_client_outlier_detection_test.go
    xds_client_retry_test.go
    xds_rls_clusterspecifier_plugin_test.go
    xds_security_config_nack_test.go
    xds_server_integration_test.go
    xds_server_rbac_test.go
    xds_server_serving_mode_test.go
)

END()
