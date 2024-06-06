GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    tab_logger.go
    usage_example.go
)

END()

RECURSE(
    slog
)
