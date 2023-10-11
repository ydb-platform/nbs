OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    monitoring.go
)

END()

RECURSE(
    config
    metrics
)
