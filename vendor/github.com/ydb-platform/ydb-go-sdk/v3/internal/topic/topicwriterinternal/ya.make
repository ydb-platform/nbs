GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    encoders.go
    message.go
    public_callbacks.go
    queue.go
    writer.go
    writer_config.go
    writer_options.go
    writer_reconnector.go
    writer_single_stream.go
    writer_stream_interface.go
)

GO_TEST_SRCS(
    encoders_test.go
    queue_test.go
    raw_topic_writer_stream_mock_test.go
    writer_reconnector_test.go
    writer_reconnector_unsafe_test.go
    writer_single_stream_test.go
    writer_stream_interface_mock_test.go
    writer_test.go
)

END()

RECURSE(
    gotest
)
