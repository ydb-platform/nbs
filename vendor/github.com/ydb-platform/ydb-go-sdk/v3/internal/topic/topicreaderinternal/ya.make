GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    batched_stream_reader_interface.go
    batcher.go
    public_callbacks.go
    reader.go
    stream_reader_impl.go
    stream_reconnector.go
    topic_client_interface.go
)

GO_TEST_SRCS(
    batched_stream_reader_mock_test.go
    batcher_test.go
    raw_topic_reader_stream_mock_test.go
    reader_test.go
    stream_reader_impl_test.go
    stream_reconnector_test.go
    topic_client_interface_mock_test.go
    transaction_mock_test.go
)

END()

RECURSE(
    gotest
)
