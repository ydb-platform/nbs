GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    batch.go
    batched_stream_reader_interface.go
    batcher.go
    commit_range.go
    committer.go
    decoders.go
    grpc_synced_stream.go
    message.go
    message_content_pool.go
    one_time_reader.go
    partition_session.go
    public_callbacks.go
    reader.go
    stream_reader_impl.go
    stream_reconnector.go
)

GO_TEST_SRCS(
    batch_test.go
    batched_stream_reader_mock_test.go
    batcher_test.go
    commit_range_test.go
    committer_test.go
    message_content_pool_test.go
    one_time_reader_test.go
    pool_interface_mock_test.go
    raw_topic_reader_stream_mock_test.go
    reader_test.go
    stream_reader_impl_test.go
    stream_reconnector_test.go
)

END()

RECURSE(
    gotest
)
