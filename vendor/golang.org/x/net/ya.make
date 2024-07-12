SUBSCRIBER(g:go-contrib)

RECURSE(
    bpf
    context
    dict
    dns
    html
    http
    http2
    icmp
    idna
    internal
    ipv4
    ipv6
    nettest
    netutil
    proxy
    publicsuffix
    quic
    trace
    webdav
    websocket
    xsrftoken
)

IF (OS_DARWIN)
    RECURSE(
        route
    )
ENDIF()
