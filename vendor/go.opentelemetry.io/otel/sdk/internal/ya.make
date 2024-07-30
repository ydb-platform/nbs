GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    gen.go
    internal.go
)

END()

RECURSE(
    env
    internaltest
    matchers
)
