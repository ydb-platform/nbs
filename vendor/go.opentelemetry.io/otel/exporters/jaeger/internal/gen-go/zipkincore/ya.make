GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    GoUnusedProtection__.go
    zipkincore-consts.go
    zipkincore.go
)

END()

RECURSE(
    zipkin_collector-remote
)
