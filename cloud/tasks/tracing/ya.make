GO_LIBRARY()

SRCS(
    init.go
    sampler.go
    tracing_context.go
)

END()

RECURSE(
    config
)
