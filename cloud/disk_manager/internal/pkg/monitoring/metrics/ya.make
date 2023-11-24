OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    interface.go
    empty.go
    buckets.go
)

END()

RECURSE(
    mocks
)
