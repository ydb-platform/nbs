GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    common.go
)

END()

RECURSE(
    authinfo
    conn
    handshaker
    proto
    testutil
    # yo
)
