GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    net.go
)

END()

RECURSE(
    connctx
    deadline
    dpipe
    netctx
    packetio
    replaydetector
    stdnet
    test
    udp
    utils
    vnet
)
