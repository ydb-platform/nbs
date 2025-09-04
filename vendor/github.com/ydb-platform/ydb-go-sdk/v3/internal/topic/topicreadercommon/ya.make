GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    batch.go
    commit_range.go
    committer.go
    const.go
    decoders.go
    errors.go
    init_message.go
    message.go
    message_content_pool.go
    one_time_reader.go
    partition_session_storage.go
    raw_topicreader_stream.go
    read_partition_session.go
    read_selector.go
    reader_id.go
    split_batches.go
)

GO_TEST_SRCS(
    batch_test.go
    commit_range_test.go
    committer_test.go
    message_content_pool_test.go
    one_time_reader_test.go
    pool_interface_mock_test.go
)

END()

RECURSE(
    gotest
)
