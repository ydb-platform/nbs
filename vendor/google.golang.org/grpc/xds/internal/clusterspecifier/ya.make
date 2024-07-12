GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    cluster_specifier.go
)

END()

RECURSE(
    rls
)
