GO_LIBRARY()

SRCS(
    monitoring.go
)

END()

RECURSE(
    config
    metrics
)
