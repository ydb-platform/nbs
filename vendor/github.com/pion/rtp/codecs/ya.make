GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    av1_packet.go
    codecs.go
    common.go
    error.go
    g711_packet.go
    g722_packet.go
    h264_packet.go
    h265_packet.go
    opus_packet.go
    vp8_packet.go
    vp9_packet.go
)

GO_TEST_SRCS(
    av1_packet_test.go
    common_test.go
    g711_packet_test.go
    g722_packet_test.go
    h264_packet_test.go
    h265_packet_test.go
    opus_packet_test.go
    vp8_packet_test.go
    vp9_packet_test.go
)

END()

RECURSE(
    av1
    gotest
)
