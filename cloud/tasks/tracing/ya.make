GO_LIBRARY()

SRCS(
    attributes.go
    init.go
    tracing_context.go
)

END()

RECURSE(
    config
)
