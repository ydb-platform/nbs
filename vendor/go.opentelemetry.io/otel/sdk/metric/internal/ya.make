GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    reuse_slice.go
)

END()

RECURSE(
    aggregate
    x
)
