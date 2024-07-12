GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    filter.go
    format.go
    option.go
    packet_dump.go
    packet_dumper.go
    receiver_interceptor.go
    sender_interceptor.go
)

GO_TEST_SRCS(
    receiver_interceptor_test.go
    sender_interceptor_test.go
)

END()

RECURSE(
    gotest
)
