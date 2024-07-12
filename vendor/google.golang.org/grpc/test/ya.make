GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    clienttester.go
    logging.go
    parse_config.go
    rawConnWrapper.go
    servertester.go
    timeouts.go
)

GO_TEST_SRCS(
    balancer_switching_test.go
    balancer_test.go
    channelz_test.go
    clientconn_state_transition_test.go
    clientconn_test.go
    compressor_test.go
    config_selector_test.go
    context_canceled_test.go
    control_plane_status_test.go
    creds_test.go
    end2end_test.go
    goaway_test.go
    gracefulstop_test.go
    healthcheck_test.go
    http_header_end2end_test.go
    idleness_test.go
    insecure_creds_test.go
    interceptor_test.go
    invoke_test.go
    local_creds_test.go
    metadata_test.go
    pickfirst_test.go
    resolver_update_test.go
    retry_test.go
    roundrobin_test.go
    server_test.go
    service_config_deprecated_test.go
    stream_cleanup_test.go
    subconn_test.go
    transport_test.go
)

IF (OS_LINUX)
    GO_TEST_SRCS(
        authority_test.go
        channelz_linux_test.go
    )
ENDIF()

END()

RECURSE(
    bufconn
    codec_perf
    gotest
    xds
)
