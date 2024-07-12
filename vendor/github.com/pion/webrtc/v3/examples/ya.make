GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    examples.go
)

END()

RECURSE(
    bandwidth-estimation-from-disk
    broadcast
    custom-logger
    data-channels
    data-channels-detach
    data-channels-flow-control
    ice-restart
    ice-single-port
    ice-tcp
    insertable-streams
    internal
    ortc
    pion-to-pion
    play-from-disk
    play-from-disk-renegotiation
    reflect
    rtcp-processing
    rtp-forwarder
    rtp-to-webrtc
    save-to-disk
    save-to-disk-av1
    simulcast
    stats
    swap-tracks
    trickle-ice
    vnet
)
