GO_LIBRARY()

SRCS(
    attributes.go
    init.go
    sampling.go
    tracing_context.go
)

END()

RECURSE(
    config
)
