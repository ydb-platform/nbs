OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    shallow_source.go
    source.go
    target.go
)

END()

RECURSE(
    config
    storage
)
