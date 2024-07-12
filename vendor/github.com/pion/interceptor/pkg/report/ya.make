GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    receiver_interceptor.go
    receiver_option.go
    receiver_stream.go
    report.go
    sender_interceptor.go
    sender_option.go
    sender_stream.go
    ticker.go
)

GO_TEST_SRCS(
    receiver_interceptor_test.go
    receiver_stream_test.go
    sender_interceptor_test.go
)

END()

RECURSE(
    gotest
)
