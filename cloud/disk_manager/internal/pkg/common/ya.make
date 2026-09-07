GO_LIBRARY()

SET(
    GO_VET_FLAGS
    -printf=false
)

SRCS(
    assert.go
    channel_with_cancellation.go
    channel_with_inflight_queue.go
    cond.go
    disk_kind.go
    errors.go
    grpc_client_tls_provider.go
    grpc_server_tls_provider.go
    inflight_queue.go
    progress_saver.go
    util.go
)

GO_TEST_SRCS(
    grpc_tls_provider_test.go
    inflight_queue_test.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
