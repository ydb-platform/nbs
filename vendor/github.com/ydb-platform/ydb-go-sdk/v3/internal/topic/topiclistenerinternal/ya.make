GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    event_handler.go
    listener_config.go
    partition_worker.go
    stream_listener.go
    topic_client.go
    topic_listener_reconnector.go
)

GO_TEST_SRCS(
    event_handler_mock_test.go
    partition_worker_mock_test.go
    partition_worker_test.go
    stream_listener_fixtures_test.go
    stream_listener_test.go
)

END()

RECURSE(
    gotest
)
