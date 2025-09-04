GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    codec.go
    message_metadata.go
    offset.go
    rangeoffset.go
    server_message_metadata.go
    transaction.go
    update_token.go
)

GO_TEST_SRCS(
    codec_test.go
    server_message_metadata_test.go
)

END()

RECURSE(
    gotest
)
