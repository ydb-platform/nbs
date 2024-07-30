GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    GoUnusedProtection__.go
    jaeger-consts.go
    jaeger.go
)

END()

RECURSE(
    collector-remote
)
