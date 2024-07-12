GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    internal.go
)

END()

RECURSE(
    eventlog
)
