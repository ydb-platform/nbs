GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    net.go
)

END()

RECURSE(
    deadline
    netctx
    test
    vnet
)
