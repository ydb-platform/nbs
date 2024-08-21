GO_LIBRARY()

SRCS(
    init.go
    sampling.go
    tracing_context.go
)

END()

RECURSE(
    config
)
