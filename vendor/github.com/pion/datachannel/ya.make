GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    datachannel.go
    errors.go
    message.go
    message_channel_ack.go
    message_channel_open.go
)

GO_TEST_SRCS(
    datachannel_test.go
    message_test.go
)

END()

RECURSE(
    gotest
)
