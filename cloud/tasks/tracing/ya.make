GO_LIBRARY()

SRCS(
    init.go
    tracing_context.go
)

END()

RECURSE(
    config
)
