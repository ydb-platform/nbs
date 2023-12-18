GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    codec.go
    server_message_metadata.go
    update_token.go
)

GO_TEST_SRCS(codec_test.go)

END()

RECURSE(
    gotest
)
