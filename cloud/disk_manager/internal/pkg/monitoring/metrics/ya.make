OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    solomon.go
    interface.go
    empty.go
    buckets.go
)

END()

RECURSE(
    mocks
)
