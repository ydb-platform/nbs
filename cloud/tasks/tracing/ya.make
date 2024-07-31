GO_LIBRARY()

SRCS(
    init.go
    trace_context.go
)

END()

RECURSE(
    config
)
