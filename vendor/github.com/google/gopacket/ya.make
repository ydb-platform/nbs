GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    base.go
    decode.go
    doc.go
    flows.go
    layerclass.go
    layers_decoder.go
    layertype.go
    packet.go
    parser.go
    time.go
    writer.go
)

GO_TEST_SRCS(
    benchmark_test.go
    packet_test.go
    time_test.go
    writer_test.go
)

END()

RECURSE(
    bytediff
    defrag
    dumpcommand
    # examples
    gotest
    ip4defrag
    layers
    macs
    pcap
    pcapgo
    # pfring
    reassembly
    routing
    tcpassembly
)

IF (OS_LINUX)
    RECURSE(
        afpacket
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        bsdbpf
    )
ENDIF()
