GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    alter_topic.go
    client.go
    controlplane_types.go
    create_topic.go
    describe_consumer.go
    describe_topic.go
    drop_topic.go
    update_offset_in_transaction.go
)

GO_TEST_SRCS(update_offset_in_transaction_test.go)

END()

RECURSE(
    gotest
    rawtopiccommon
    rawtopicreader
    rawtopicreadermock
    rawtopicwriter
)
