GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    ack_delay.go
    acks.go
    atomic_bits.go
    config.go
    congestion_reno.go
    conn.go
    conn_close.go
    conn_flow.go
    conn_id.go
    conn_loss.go
    conn_recv.go
    conn_send.go
    conn_streams.go
    crypto_stream.go
    dgram.go
    doc.go
    endpoint.go
    errors.go
    frame_debug.go
    gate.go
    idle.go
    log.go
    loss.go
    math.go
    pacer.go
    packet.go
    packet_number.go
    packet_parser.go
    packet_protection.go
    packet_writer.go
    path.go
    ping.go
    pipe.go
    qlog.go
    queue.go
    quic.go
    rangeset.go
    retry.go
    rtt.go
    sent_packet.go
    sent_packet_list.go
    sent_val.go
    stateless_reset.go
    stream.go
    stream_limits.go
    tls.go
    transport_params.go
    udp.go
    wire.go
)

IF (OS_LINUX)
    SRCS(
        udp_linux.go
        udp_msg.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        udp_darwin.go
        udp_msg.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        udp_other.go
    )
ENDIF()

END()

RECURSE(
    qlog
)
