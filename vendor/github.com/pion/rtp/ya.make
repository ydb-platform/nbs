GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    abscapturetimeextension.go
    abssendtimeextension.go
    audiolevelextension.go
    depacketizer.go
    error.go
    header_extension.go
    packet.go
    packetizer.go
    partitionheadchecker.go
    payload_types.go
    playoutdelayextension.go
    rand.go
    rtp.go
    sequencer.go
    transportccextension.go
    vlaextension.go
)

GO_TEST_SRCS(
    abscapturetimeextension_test.go
    abssendtimeextension_test.go
    audiolevelextension_test.go
    header_extension_test.go
    packet_test.go
    packetizer_test.go
    playoutdelayextension_test.go
    transportccextension_test.go
    vlaextension_test.go
)

END()

RECURSE(
    codecs
    gotest
    pkg
)
