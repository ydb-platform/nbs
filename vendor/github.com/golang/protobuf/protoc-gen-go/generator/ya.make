GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    generator.go
)

END()

RECURSE(
    internal
)
